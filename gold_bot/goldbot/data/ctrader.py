"""Adapter cTrader Open API - TYLKO ODCZYT (notowania bid/ask i specyfikacja symbolu).

Wymagania (poza biblioteką standardową): `pip install ctrader-open-api`
Wcześniej: rejestracja aplikacji Open API i akceptacja Spotware, autoryzacja konta demo
z uprawnieniem (scope) "accounts" - bez "trading". Tokeny trzymamy w zmiennych środowiskowych.

Zabezpieczenie: `_send` przepuszcza wyłącznie komunikaty z listy ALLOWED_REQUESTS.
Żadne zlecenie handlowe nie może zostać wysłane z tego modułu.

UWAGA: kod napisany na podstawie dokumentacji SDK, nie był uruchomiony na koncie demo.
Przy pierwszym uruchomieniu zweryfikuj nazwy pól (lotSize, minVolume, stepVolume, digits)
oraz jednostki (ceny w 1/100000, wolumeny w setnych jednostki).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Callable

from goldbot.models import InstrumentSpec, Quote

ALLOWED_REQUESTS = frozenset({
    "ProtoOAApplicationAuthReq",
    "ProtoOAAccountAuthReq",
    "ProtoOASymbolsListReq",
    "ProtoOASymbolByIdReq",
    "ProtoOASubscribeSpotsReq",
    "ProtoOAUnsubscribeSpotsReq",
    "ProtoOAGetTrendbarsReq",
    "ProtoHeartbeatEvent",
})

PRICE_SCALE = 100000.0


class ReadOnlyViolation(RuntimeError):
    pass


class CTraderReadOnlyFeed:
    def __init__(self, on_quote: Callable[[Quote], None], on_spec: Callable[[InstrumentSpec], None] | None = None,
                 on_disconnect: Callable[[str], None] | None = None, symbol_names: tuple[str, ...] = ("XAUUSD", "GOLD"),
                 demo: bool = True):
        self.client_id = os.environ["CTRADER_CLIENT_ID"]
        self.client_secret = os.environ["CTRADER_CLIENT_SECRET"]
        self.access_token = os.environ["CTRADER_ACCESS_TOKEN"]
        self.account_id = int(os.environ["CTRADER_ACCOUNT_ID"])
        self.on_quote, self.on_spec, self.on_disconnect = on_quote, on_spec, on_disconnect
        self.symbol_names = tuple(s.upper() for s in symbol_names)
        self.demo = demo
        self.symbol_id: int | None = None
        self._bid: float | None = None
        self._ask: float | None = None
        self._client = None
        self._m = None  # moduł komunikatów protobuf

    def _send(self, msg) -> None:
        name = type(msg).__name__
        if name not in ALLOWED_REQUESTS:
            raise ReadOnlyViolation(f"Adapter jest tylko do odczytu - zablokowano {name}")
        self._client.send(msg)

    def start(self) -> None:
        from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
        from ctrader_open_api.messages import OpenApiMessages_pb2 as m
        from twisted.internet import reactor

        self._m, self._protobuf = m, Protobuf
        host = EndPoints.PROTOBUF_DEMO_HOST if self.demo else EndPoints.PROTOBUF_LIVE_HOST
        self._client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self._client.setConnectedCallback(self._on_connected)
        self._client.setDisconnectedCallback(lambda c, reason: self.on_disconnect and self.on_disconnect(str(reason)))
        self._client.setMessageReceivedCallback(self._on_message)
        self._client.startService()
        reactor.run()

    def _on_connected(self, client) -> None:
        req = self._m.ProtoOAApplicationAuthReq()
        req.clientId, req.clientSecret = self.client_id, self.client_secret
        self._send(req)

    def _on_message(self, client, message) -> None:
        m = self._m
        msg = self._protobuf.extract(message)
        name = type(msg).__name__
        if name == "ProtoOAApplicationAuthRes":
            req = m.ProtoOAAccountAuthReq()
            req.ctidTraderAccountId, req.accessToken = self.account_id, self.access_token
            self._send(req)
        elif name == "ProtoOAAccountAuthRes":
            req = m.ProtoOASymbolsListReq()
            req.ctidTraderAccountId = self.account_id
            self._send(req)
        elif name == "ProtoOASymbolsListRes":
            for s in msg.symbol:
                if s.symbolName.upper() in self.symbol_names:
                    self.symbol_id = s.symbolId
                    break
            if self.symbol_id is None:
                raise RuntimeError(f"Nie znaleziono symbolu {self.symbol_names}")
            req = m.ProtoOASymbolByIdReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId.append(self.symbol_id)
            self._send(req)
        elif name == "ProtoOASymbolByIdRes":
            s = msg.symbol[0]
            if self.on_spec:
                # wolumeny w cTrader są w setnych jednostki; lotSize = jednostek na lot * 100
                contract = s.lotSize / 100.0
                self.on_spec(InstrumentSpec(
                    symbol="XAUUSD",
                    contract_size=contract,
                    min_volume_lots=(s.minVolume / 100.0) / contract,
                    volume_step_lots=(s.stepVolume / 100.0) / contract,
                    max_volume_lots=(s.maxVolume / 100.0) / contract,
                ))
            req = m.ProtoOASubscribeSpotsReq()
            req.ctidTraderAccountId = self.account_id
            req.symbolId.append(self.symbol_id)
            self._send(req)
        elif name == "ProtoOASpotEvent" and msg.symbolId == self.symbol_id:
            # zdarzenie może zawierać tylko zmienione pole - trzymamy ostatnie wartości
            if msg.HasField("bid"):
                self._bid = msg.bid / PRICE_SCALE
            if msg.HasField("ask"):
                self._ask = msg.ask / PRICE_SCALE
            now = datetime.now(timezone.utc)
            ts = datetime.fromtimestamp(msg.timestamp / 1000, timezone.utc) if msg.HasField("timestamp") else now
            if self._bid is not None and self._ask is not None:
                self.on_quote(Quote(ts, self._bid, self._ask, received_at=now, source="ctrader"))
        elif name == "ProtoOAErrorRes":
            raise RuntimeError(f"cTrader error: {msg.errorCode} {msg.description}")
