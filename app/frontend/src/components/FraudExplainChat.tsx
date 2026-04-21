import { useCallback, useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import type { ChatMessage, ScoredTransaction } from "../types";

interface FraudExplainChatProps {
  selectedTxn: ScoredTransaction | null;
}

function formatAmount(n: number): string {
  return n.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

export function FraudExplainChat({ selectedTxn }: FraudExplainChatProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const scrollerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    setMessages([]);
    setInput("");
  }, [selectedTxn?.id]);

  useEffect(() => {
    if (scrollerRef.current) {
      scrollerRef.current.scrollTop = scrollerRef.current.scrollHeight;
    }
  }, [messages, sending]);

  const send = useCallback(async () => {
    const trimmed = input.trim();
    if (!trimmed || sending) return;

    const userMsg: ChatMessage = { role: "user", content: trimmed };
    const next = [...messages, userMsg];
    setMessages(next);
    setInput("");
    setSending(true);
    try {
      const res = await fetch("/api/explain-fraud", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: next }),
      });
      if (!res.ok) throw new Error(`agent ${res.status}`);
      const body = (await res.json()) as ChatMessage;
      setMessages([...next, { role: "assistant", content: body.content }]);
    } catch {
      setMessages([
        ...next,
        {
          role: "assistant",
          content:
            "Agent call failed. Check the app logs (/logz) and confirm the bank-fraud-explain resource is bound.",
        },
      ]);
    } finally {
      setSending(false);
    }
  }, [input, messages, sending]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        send();
      }
    },
    [send]
  );

  const clearChat = useCallback(() => {
    setMessages([]);
    setInput("");
  }, []);

  return (
    <div className="bg-white/5 rounded-lg border border-white/10 overflow-hidden h-full flex flex-col">
      <div className="px-4 py-3 border-b border-white/10 flex items-center justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-white/80">
            Fraud Explanation Agent
          </h2>
          {selectedTxn ? (
            <div className="text-[11px] text-white/50 truncate mt-0.5">
              <span className="font-mono">{selectedTxn.id}</span> ·{" "}
              {selectedTxn.type} · {formatAmount(selectedTxn.amount)} ·{" "}
              {selectedTxn.country_orig} → {selectedTxn.country_dest}
            </div>
          ) : (
            <div className="text-[11px] text-white/40 mt-0.5">
              Select a fraud transaction on the left to begin.
            </div>
          )}
        </div>
        <button
          onClick={clearChat}
          disabled={messages.length === 0 && !input}
          className="text-xs text-white/50 hover:text-white/80 disabled:opacity-30 disabled:cursor-not-allowed"
        >
          Clear chat
        </button>
      </div>

      <div ref={scrollerRef} className="flex-1 overflow-auto px-4 py-3 space-y-3">
        {messages.length === 0 && !sending && (
          <div className="text-xs text-white/30 text-center py-8">
            {selectedTxn
              ? `Ask the agent about live txn ${selectedTxn.id}, e.g. "Why does this look risky?"`
              : ""}
          </div>
        )}
        {messages.map((m, i) => (
          <div
            key={i}
            className={`flex ${
              m.role === "user" ? "justify-end" : "justify-start"
            }`}
          >
            <div
              className={`max-w-[85%] rounded-lg px-3 py-2 text-sm ${
                m.role === "user"
                  ? "bg-dbx-red/80 text-white whitespace-pre-wrap"
                  : "bg-white/10 text-white/90 markdown-body"
              }`}
            >
              {m.role === "assistant" ? (
                <ReactMarkdown>{m.content}</ReactMarkdown>
              ) : (
                m.content
              )}
            </div>
          </div>
        ))}
        {sending && (
          <div className="flex justify-start">
            <div className="rounded-lg px-3 py-2 text-sm bg-white/10 text-white/60 italic flex items-center gap-2">
              <span className="inline-block w-1.5 h-1.5 bg-white/60 rounded-full animate-pulse" />
              <span
                className="inline-block w-1.5 h-1.5 bg-white/60 rounded-full animate-pulse"
                style={{ animationDelay: "150ms" }}
              />
              <span
                className="inline-block w-1.5 h-1.5 bg-white/60 rounded-full animate-pulse"
                style={{ animationDelay: "300ms" }}
              />
              <span className="ml-1">agent thinking…</span>
            </div>
          </div>
        )}
      </div>

      <div className="border-t border-white/10 p-3 flex items-end gap-2">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={
            selectedTxn
              ? `Ask about live txn ${selectedTxn.id}, or another id...`
              : "Select a transaction to begin..."
          }
          disabled={!selectedTxn || sending}
          rows={2}
          className="flex-1 resize-none bg-white/5 border border-white/10 rounded px-3 py-2 text-sm text-white placeholder-white/30 focus:outline-none focus:border-dbx-red/50 disabled:opacity-40"
        />
        <button
          onClick={send}
          disabled={!input.trim() || sending || !selectedTxn}
          className="px-4 py-2 rounded text-sm font-medium bg-dbx-red text-white hover:bg-dbx-red/90 disabled:opacity-30 disabled:cursor-not-allowed transition-all"
        >
          Send
        </button>
      </div>
    </div>
  );
}
