"""Twarde reguły ryzyka - niezależne od strategii i od AI."""

from __future__ import annotations

import math
from dataclasses import dataclass

from goldbot.config import RiskConfig
from goldbot.models import InstrumentSpec, Side


@dataclass
class SizingResult:
    ok: bool
    volume_oz: float = 0.0
    planned_loss: float = 0.0  # USD przy trafieniu SL, łącznie z prowizją i poślizgiem
    reason: str = ""


def floor_to_step(x: float, step: float) -> float:
    return math.floor(x / step + 1e-9) * step


def size_position(side: Side, entry: float, sl_distance: float, equity: float,
                  spec: InstrumentSpec, risk: RiskConfig, free_margin: float) -> SizingResult:
    """Dobiera wolumen tak, aby strata na SL (z kosztami) nie przekroczyła limitu.

    Jeżeli nawet minimalny wolumen brokera przekracza limit - transakcja jest odrzucana.
    Nie tworzymy ułamków pozycji, których broker nie pozwoli otworzyć.
    """
    if sl_distance <= 0:
        return SizingResult(False, reason="invalid_sl_distance")
    budget = equity * risk.risk_per_trade_pct / 100.0
    commission_per_oz = 2 * spec.commission_per_lot_per_side / spec.contract_size + 2 * spec.commission_pct_per_side / 100.0 * entry
    # wejście i wyjście po stopie: poślizg dwukrotnie
    loss_per_oz = sl_distance + 2 * spec.slippage_per_oz + commission_per_oz
    step, min_oz = spec.volume_step_oz, spec.min_volume_oz
    vol = floor_to_step(budget / loss_per_oz, step)
    vol = min(vol, spec.max_volume_lots * spec.contract_size)
    min_loss = min_oz * loss_per_oz
    if vol < min_oz - 1e-9:
        return SizingResult(False, planned_loss=round(min_loss, 2),
                            reason=f"min_volume_exceeds_risk (min {min_oz:g} oz => strata {min_loss:.2f} USD > limit {budget:.2f} USD)")
    margin_cap = free_margin * risk.max_margin_usage_pct / 100.0
    margin_per_oz = entry / spec.leverage
    if vol * margin_per_oz > margin_cap:
        vol = floor_to_step(margin_cap / margin_per_oz, step)
        if vol < min_oz - 1e-9:
            return SizingResult(False, reason=f"insufficient_margin (potrzeba {min_oz * margin_per_oz:.2f} USD, limit {margin_cap:.2f} USD)")
    return SizingResult(True, volume_oz=round(vol, 6), planned_loss=round(vol * loss_per_oz, 2))
