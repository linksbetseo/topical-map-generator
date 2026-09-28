import { redactSecrets } from "@solbot/config";
import type { Notifier, NotifyEvent } from "./ports.ts";

/**
 * Outbound-only Telegram notifications to one authorized chat id. v1 handles no inbound commands at all
 * (no live toggle, no signing, no withdrawals, no size changes). Plain text (no parse_mode) so token
 * metadata cannot inject markup.
 */
export function formatNotification(e: NotifyEvent): string {
  switch (e.kind) {
    case "ENTRY":
      return `[PAPER] Wejście ${e.mint}: ${e.notionalUsd.toFixed(2)} USD (fill ${e.fillId}, symulacja z quote)`;
    case "EXIT":
      return `[PAPER] Wyjście ${e.mint}: ${e.reason}, PnL ${e.pnlUsd.toFixed(2)} USD (fill ${e.fillId})`;
    case "STATE":
      return `[PAPER] Stan sesji: ${e.from} -> ${e.to}${e.reason ? ` (${e.reason})` : ""}`;
    case "ALERT":
      return `[PAPER] ${e.severity}: ${e.message}`;
    case "REPORT":
      return `[PAPER] ${e.title}\n${e.body}`;
  }
}

export class TelegramNotifier implements Notifier {
  constructor(
    private readonly token: string,
    private readonly chatId: string,
    private readonly fetchImpl: typeof fetch = fetch,
    private readonly onError: (msg: string) => void = () => undefined,
  ) {}

  async notify(e: NotifyEvent): Promise<void> {
    const text = formatNotification(e).slice(0, 3_900);
    try {
      const res = await this.fetchImpl(`https://api.telegram.org/bot${this.token}/sendMessage`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ chat_id: this.chatId, text, disable_web_page_preview: true }),
      });
      if (!res.ok) this.onError(`telegram HTTP ${res.status}`);
    } catch (err) {
      // never leak the bot token (it is part of the URL)
      this.onError(redactSecrets(err instanceof Error ? err.message : String(err), [this.token]));
    }
  }
}
