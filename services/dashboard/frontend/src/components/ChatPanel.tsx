import React, { useState, useEffect, useRef, useCallback } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ToolCall {
  id: string; // generated client-side to match result
  name: string;
  input: Record<string, unknown>;
  result?: unknown;
  resultExpanded?: boolean;
}

interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  text: string;
  toolCalls: ToolCall[];
  streaming: boolean;
  error?: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function generateId(): string {
  return Math.random().toString(36).slice(2, 10);
}

function getSessionId(): string {
  const key = "chat_session_id";
  let id = localStorage.getItem(key);
  if (!id) {
    // Simple UUID v4-like
    id = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      const v = c === "x" ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
    localStorage.setItem(key, id);
  }
  return id;
}

const QUICK_SUGGESTIONS = [
  "Should I go long now?",
  "What's the latest @marketfeed sentiment?",
  "Show me my open positions",
];

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function ToolCallCard({
  toolCall,
  onToggleResult,
}: {
  toolCall: ToolCall;
  onToggleResult: () => void;
}) {
  return (
    <div className="mt-2 border border-gray-600 rounded-lg overflow-hidden text-xs">
      {/* Tool call header */}
      <div className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-700/60">
        <span className="text-gray-400">Tool</span>
        <span className="font-mono font-semibold text-yellow-300">{toolCall.name}</span>
        <span className="text-gray-500 font-mono truncate max-w-[200px]">
          ({JSON.stringify(toolCall.input).slice(0, 60)}
          {JSON.stringify(toolCall.input).length > 60 ? "…" : ""})
        </span>
      </div>
      {/* Tool result (if available) */}
      {toolCall.result !== undefined && (
        <div>
          <button
            onClick={onToggleResult}
            className="w-full text-left px-3 py-1 bg-gray-700/30 text-gray-500 hover:text-gray-300 transition-colors flex items-center gap-1"
          >
            <svg
              className={`w-3 h-3 transition-transform ${toolCall.resultExpanded ? "rotate-90" : ""}`}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
            Result
          </button>
          {toolCall.resultExpanded && (
            <pre className="px-3 py-2 bg-gray-900 text-[10px] text-green-300 overflow-x-auto max-h-40 font-mono">
              {typeof toolCall.result === "string"
                ? toolCall.result
                : JSON.stringify(toolCall.result, null, 2)}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

function UserBubble({ message }: { message: ChatMessage }) {
  return (
    <div className="flex justify-end">
      <div className="max-w-[85%] bg-blue-600 text-white rounded-2xl rounded-tr-sm px-4 py-2.5 text-sm leading-relaxed">
        {message.text}
      </div>
    </div>
  );
}

function AssistantBubble({
  message,
  onToggleToolResult,
}: {
  message: ChatMessage;
  onToggleToolResult: (msgId: string, toolId: string) => void;
}) {
  return (
    <div className="flex justify-start">
      <div className="max-w-[90%] space-y-1.5">
        <div className="bg-gray-700 text-gray-100 rounded-2xl rounded-tl-sm px-4 py-2.5 text-sm leading-relaxed">
          {message.text || (message.streaming ? "" : <span className="text-gray-500 italic">…</span>)}
          {message.streaming && (
            <span className="inline-block w-1.5 h-4 bg-blue-400 animate-pulse ml-0.5 align-middle" />
          )}
        </div>
        {message.toolCalls.map((tc) => (
          <ToolCallCard
            key={tc.id}
            toolCall={tc}
            onToggleResult={() => onToggleToolResult(message.id, tc.id)}
          />
        ))}
        {message.error && (
          <div className="bg-red-900/40 border border-red-700 rounded-lg px-3 py-1.5 text-xs text-red-300">
            {message.error}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Chat icon SVG
// ---------------------------------------------------------------------------

function ChatIcon() {
  return (
    <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z"
      />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Main ChatPanel
// ---------------------------------------------------------------------------

const ChatPanel: React.FC = () => {
  const [expanded, setExpanded] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const sessionId = useRef(getSessionId());
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Auto-scroll to bottom when messages update
  useEffect(() => {
    if (expanded) {
      messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages, expanded]);

  const toggleToolResult = useCallback((msgId: string, toolId: string) => {
    setMessages((prev) =>
      prev.map((m) =>
        m.id === msgId
          ? {
              ...m,
              toolCalls: m.toolCalls.map((tc) =>
                tc.id === toolId
                  ? { ...tc, resultExpanded: !tc.resultExpanded }
                  : tc
              ),
            }
          : m
      )
    );
  }, []);

  const sendMessage = useCallback(
    async (text: string) => {
      if (!text.trim() || sending) return;

      const userMsg: ChatMessage = {
        id: generateId(),
        role: "user",
        text: text.trim(),
        toolCalls: [],
        streaming: false,
      };

      const assistantMsgId = generateId();
      const assistantMsg: ChatMessage = {
        id: assistantMsgId,
        role: "assistant",
        text: "",
        toolCalls: [],
        streaming: true,
      };

      setMessages((prev) => [...prev, userMsg, assistantMsg]);
      setInput("");
      setSending(true);

      // Map tool_call name→id for result matching
      const toolCallNameToId = new Map<string, string>();

      try {
        const response = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: text.trim(),
            session_id: sessionId.current,
          }),
        });

        if (!response.body) {
          throw new Error("No response body");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        const handleEvent = (eventType: string, data: Record<string, unknown>) => {
          setMessages((prev) =>
            prev.map((m) => {
              if (m.id !== assistantMsgId) return m;

              switch (eventType) {
                case "token": {
                  return { ...m, text: m.text + ((data.text as string) ?? "") };
                }
                case "tool_call": {
                  const tcId = generateId();
                  const tcName = (data.name as string) ?? "unknown";
                  toolCallNameToId.set(tcName, tcId);
                  const newTc: ToolCall = {
                    id: tcId,
                    name: tcName,
                    input: (data.input as Record<string, unknown>) ?? {},
                  };
                  return { ...m, toolCalls: [...m.toolCalls, newTc] };
                }
                case "tool_result": {
                  const resultName = (data.name as string) ?? "";
                  const tcId = toolCallNameToId.get(resultName);
                  if (!tcId) return m;
                  return {
                    ...m,
                    toolCalls: m.toolCalls.map((tc) =>
                      tc.id === tcId
                        ? { ...tc, result: data.output, resultExpanded: false }
                        : tc
                    ),
                  };
                }
                case "done": {
                  return { ...m, streaming: false };
                }
                case "error": {
                  return {
                    ...m,
                    streaming: false,
                    error: (data.error as string) ?? "Unknown error",
                  };
                }
                default:
                  return m;
              }
            })
          );
        };

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          const events = buffer.split("\n\n");
          buffer = events.pop() ?? "";

          for (const evt of events) {
            const lines = evt.split("\n");
            const eventType = lines
              .find((l) => l.startsWith("event:"))
              ?.slice(6)
              .trim();
            const dataLine = lines
              .find((l) => l.startsWith("data:"))
              ?.slice(5)
              .trim();
            if (!eventType || !dataLine) continue;
            try {
              const data = JSON.parse(dataLine) as Record<string, unknown>;
              handleEvent(eventType, data);
            } catch {
              // ignore malformed SSE data
            }
          }
        }

        // Finalize streaming in case done event wasn't received
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantMsgId ? { ...m, streaming: false } : m
          )
        );
      } catch (err) {
        const errMsg = err instanceof Error ? err.message : String(err);
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantMsgId
              ? { ...m, streaming: false, error: errMsg }
              : m
          )
        );
      } finally {
        setSending(false);
      }
    },
    [sending]
  );

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      void sendMessage(input);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    void sendMessage(input);
  };

  return (
    <>
      {/* Collapsed button */}
      {!expanded && (
        <button
          onClick={() => setExpanded(true)}
          className="fixed bottom-6 right-6 z-50 w-14 h-14 rounded-full bg-blue-600 hover:bg-blue-500 text-white shadow-2xl flex items-center justify-center transition-all hover:scale-105 active:scale-95"
          aria-label="Open chat"
        >
          <ChatIcon />
        </button>
      )}

      {/* Expanded panel */}
      {expanded && (
        <div className="fixed bottom-6 right-6 z-50 w-[400px] h-[600px] bg-gray-900 border border-gray-700 rounded-2xl shadow-2xl flex flex-col overflow-hidden">
          {/* Panel header */}
          <div className="flex-shrink-0 flex items-center justify-between px-4 py-3 border-b border-gray-800 bg-gray-900">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-blue-400 animate-pulse" />
              <span className="text-sm font-semibold text-gray-200">Bot Assistant</span>
            </div>
            <button
              onClick={() => setExpanded(false)}
              className="text-gray-500 hover:text-gray-200 transition-colors p-1 rounded"
              aria-label="Close chat"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>
          </div>

          {/* Messages area */}
          <div className="flex-1 overflow-y-auto p-4 space-y-3">
            {messages.length === 0 && (
              <div className="space-y-3">
                <p className="text-xs text-gray-500 text-center">
                  Ask the trading bot anything
                </p>
                <div className="space-y-2">
                  {QUICK_SUGGESTIONS.map((s) => (
                    <button
                      key={s}
                      onClick={() => void sendMessage(s)}
                      className="w-full text-left px-3 py-2 rounded-lg border border-gray-700 bg-gray-800 hover:bg-gray-700 text-xs text-gray-300 transition-colors"
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg) =>
              msg.role === "user" ? (
                <UserBubble key={msg.id} message={msg} />
              ) : (
                <AssistantBubble
                  key={msg.id}
                  message={msg}
                  onToggleToolResult={toggleToolResult}
                />
              )
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* Input area */}
          <div className="flex-shrink-0 border-t border-gray-800 p-3">
            <form onSubmit={handleSubmit} className="flex gap-2 items-end">
              <textarea
                ref={textareaRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Message… (Enter to send, Shift+Enter for newline)"
                rows={2}
                disabled={sending}
                className="flex-1 resize-none bg-gray-800 border border-gray-700 rounded-xl px-3 py-2 text-sm text-gray-200 placeholder-gray-600 focus:outline-none focus:border-blue-500 disabled:opacity-50 min-h-[44px] max-h-24"
              />
              <button
                type="submit"
                disabled={!input.trim() || sending}
                className="flex-shrink-0 w-9 h-9 rounded-xl bg-blue-600 hover:bg-blue-500 disabled:bg-gray-700 disabled:text-gray-500 text-white flex items-center justify-center transition-colors mb-0.5"
                aria-label="Send message"
              >
                {sending ? (
                  <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
                  </svg>
                ) : (
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
                  </svg>
                )}
              </button>
            </form>
          </div>
        </div>
      )}
    </>
  );
};

export default ChatPanel;
