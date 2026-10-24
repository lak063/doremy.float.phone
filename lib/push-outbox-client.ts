// 离线推送·回端合并：App 打开/回前台时拉取服务端生成的原始输出，
// 用客户端同一条解析管线（输出正则 → parseAndSaveResponse）落进聊天记录。

import { parseAndSaveResponse, scheduleFollowUp } from "./follow-up-service";
import { applyOutputRegex } from "./llm-prompt-assembler";
import type { RegexConfig } from "./settings-types";
import { stripHallucinatedTimestamps } from "./llm-provider-adapter";
import { MacroEngine } from "./macro-engine";
import { getActiveAppTags } from "./content-tag-utils";
import { loadChatMessages, loadChatSessions, reindexSessionMessageOrdersByTime } from "./chat-storage";
import { hasAccountPushSubscription, isShellEnvironment } from "./push-client";
import { isPersonalPushCloudActive, loadPersonalPushCloudState, personalPushFetch } from "./personal-push-cloud";
import { removeTimedWakeSchedule } from "./timed-wake-storage";
import { appendBridgeFeed } from "./reality-bridge/storage";
import { loadScreenChatSettings, saveScreenChatAck } from "./reality-bridge/storage";

type OutboxEntry = {
    id: string;
    session_id: string | null;
    trigger_key: string | null;
    raw_text: string;
    meta: {
        sessionId?: string;
        followUpIndex?: number;
        prevCount?: number;
        regexes?: RegexConfig[];
        characterName?: string;
        userName?: string;
        appId?: string;
        appTags?: string[];
        followUpCount?: number;
        armAt?: string;
        /** 云端触发快捷动作失败的摘要；成功时不带这个字段 */
        shortcutDeliveryError?: string;
    } | null;
    created_at: string;
};

let consuming = false;
let lastConsumeAt = 0;
let consumerInstalled = false;
let consumeRequestTimer: number | null = null;
const OUTBOX_BATCH_SIZE = 20;
const MAX_OUTBOX_BATCHES_PER_PASS = 10;
const OUTBOX_FOREGROUND_CHECK_INTERVAL_MS = 5 * 60 * 1000;

function getTimedWakeIdFromTriggerKey(triggerKey: string | null): string | null {
    if (!triggerKey?.startsWith("timedwake:")) return null;
    const id = triggerKey.slice("timedwake:".length);
    return id.trim() || null;
}

function clearTimedWakeIfHandled(triggerKey: string | null): void {
    const timedWakeId = getTimedWakeIdFromTriggerKey(triggerKey);
    if (timedWakeId) removeTimedWakeSchedule(timedWakeId);
}

export async function consumeServerOutbox(options?: { silent?: boolean; force?: boolean }): Promise<void> {
    if (typeof window === "undefined") return;
    // 壳环境的任务与回传箱都走站点（账号 id 与壳订阅一致，见 pushJobsFetch）：
    // 服务端生成的消息落在站点库里，user_id = 站点账号 id。个人云那条路在壳里
    // 是断的（网关按 OWNER_ID 存取），所以壳必须读站点回传箱，不能读个人云。
    const shellMode = isShellEnvironment();
    // 共享回传箱已紧急停用：没有个人 Supabase 时直接结束，不请求 status/outbox。
    if (!shellMode && !isPersonalPushCloudActive()) return;
    if (consuming) return;
    if (options?.force !== true && Date.now() - lastConsumeAt < OUTBOX_FOREGROUND_CHECK_INTERVAL_MS) return;
    // 没有任何设备订阅推送时，服务端不可能产生普通离线回传；避免所有在线用户空轮询。
    if (!loadScreenChatSettings().enabled && !(await hasAccountPushSubscription())) return;
    consuming = true;
    lastConsumeAt = Date.now();
    const passStartMs = Date.now();
    try {
        // 壳读站点回传箱（与它的任务同源）；其余情况读个人云回传箱。
        const sources: Array<"personal" | "shared"> = shellMode ? ["shared"] : ["personal"];
        const handledTriggerKeys = new Set<string>();
        // 本轮开始前各会话已有的消息 id 快照。
        //
        // 为什么需要：合并现在沿用服务端生成时间（createdAt = entry.created_at），
        // 落库后这些消息的 createdAt 必然早于 passStartMs。下面按 armAt 判定
        // 「本地是否已经生成过这一轮」时，会把同一批里刚合并进来的条目误认成
        // 本地产物，于是后续条目被跳过、消息丢失。记住基线就能把它们排除掉。
        const passBaselineIds = new Map<string, Set<string>>();
        // 本轮真正合并进消息的会话，收尾时按时间重排一次 order。
        // 合并进来的消息带的是服务端生成时间（过去时刻），可能插在本轮之前
        // 已有的本地消息之间——不重排的话 order 仍是追加顺序，显示会错乱。
        const mergedSessionIds = new Set<string>();
        for (const source of sources) {
          for (let batch = 0; batch < MAX_OUTBOX_BATCHES_PER_PASS; batch += 1) {
            const response = await (source === "personal"
                ? personalPushFetch("outbox")
                : fetch("/api/push/outbox", { credentials: "include" }))
                .catch(() => null);
            if (!response || !response.ok) break;
            const data = await response.json().catch(() => ({})) as { ok?: boolean; entries?: OutboxEntry[] };
            const entries = data.ok && Array.isArray(data.entries) ? data.entries : [];
            if (entries.length === 0) break;

            const consumedIds: string[] = [];
            for (const entry of entries) {
                try {
                    if (entry.trigger_key && handledTriggerKeys.has(entry.trigger_key)) {
                        consumedIds.push(entry.id);
                        continue;
                    }
                    const meta = entry.meta || {};

                    if ((meta as { kind?: string }).kind === "bridge") {
                        const bridgeMeta = meta as Record<string, unknown> & {
                            reply?: { sessionId?: string; regexes?: RegexConfig[]; characterName?: string; userName?: string; appId?: string; appTags?: string[] } | null;
                            screenChat?: boolean;
                            screenChatCharacterId?: string;
                            screenChatSequence?: number;
                            screenChatResponseBatchId?: string;
                            screenChatAssistantAt?: string;
                        };
                        const engine = await import("./reality-bridge/engine");
                        const applied = await engine.applyServerBridgeEntry(bridgeMeta as Parameters<typeof engine.applyServerBridgeEntry>[0]);
                        const replyMeta = bridgeMeta.reply || null;
                        const replySessionId = applied.sessionId || replyMeta?.sessionId || "";
                        if (entry.raw_text.trim() && replySessionId) {
                            const responseBatchId = typeof bridgeMeta.screenChatResponseBatchId === "string"
                                ? bridgeMeta.screenChatResponseBatchId
                                : undefined;
                            const existing = loadChatMessages(replySessionId);
                            const alreadyImported = Boolean(
                                responseBatchId && existing.some(message => message.responseBatchId === responseBatchId),
                            );
                            if (!alreadyImported) {
                                let text = stripHallucinatedTimestamps(entry.raw_text.trim());
                                const regexes = Array.isArray(replyMeta?.regexes) ? replyMeta.regexes : [];
                                if (regexes.length > 0) {
                                    const macroEngine = new MacroEngine(replyMeta?.characterName ?? "", replyMeta?.userName ?? "用户");
                                    const activeTags = getActiveAppTags(replyMeta?.appId ?? "chat", { appTags: replyMeta?.appTags });
                                    text = applyOutputRegex(text, regexes, { macroEngine, activeTags });
                                }
                                const { hasVisible, newCount, stateValues } = await parseAndSaveResponse(
                                    text,
                                    replySessionId,
                                    0,
                                    undefined,
                                    existing,
                                    {
                                        silent: options?.silent !== false,
                                        responseBatchId,
                                        createdAt: bridgeMeta.screenChatAssistantAt,
                                    },
                                );
                                if (hasVisible && newCount < 10) scheduleFollowUp(replySessionId, newCount, stateValues);
                            }
                            if (bridgeMeta.screenChat === true) reindexSessionMessageOrdersByTime(replySessionId);
                        }
                        if (
                            bridgeMeta.screenChat === true
                            && typeof bridgeMeta.screenChatCharacterId === "string"
                            && Number.isSafeInteger(bridgeMeta.screenChatSequence)
                        ) {
                            saveScreenChatAck(bridgeMeta.screenChatCharacterId, Number(bridgeMeta.screenChatSequence));
                        }
                        consumedIds.push(entry.id);
                        if (entry.trigger_key) handledTriggerKeys.add(entry.trigger_key);
                        continue;
                    }
                    // 云端触发快捷动作失败的诊断行：角色已经说了"我去看一眼"却什么
                    // 都没发生，写进现实桥动态让用户查得到原因。这是一条独立的
                    // outbox 行（云端在投递之后才写），不是角色消息——消费掉即可，
                    // 绝不能走下面的建消息流程，否则聊天里会凭空多出一条。
                    if ((meta as { kind?: string }).kind === "shortcut_delivery_error") {
                        const detail = typeof meta.shortcutDeliveryError === "string"
                            ? meta.shortcutDeliveryError
                            : entry.raw_text;
                        try {
                            appendBridgeFeed({
                                id: `shortcut_fail_${entry.id}`,
                                type: "快捷动作",
                                payload: detail,
                                rules: [],
                                actions: [],
                                error: detail,
                                receivedAt: new Date().toISOString(),
                            });
                        } catch { /* 动态写失败不影响其余条目消费 */ }
                        consumedIds.push(entry.id);
                        continue;
                    }
                    const sessionId = meta.sessionId || entry.session_id || "";
                    const session = sessionId ? loadChatSessions().find(s => s.id === sessionId) : undefined;
                    if (!session) {
                        console.warn("[PushOutbox] session not found, keep entry pending:", entry.id, sessionId);
                        continue;
                    }

                    const periodCare = (meta as { periodCare?: { characterId?: string; cycleKey?: string } }).periodCare;
                    if (periodCare?.characterId && periodCare.cycleKey) {
                        const { saveMenstrualPeriodCareTrigger } = await import("./menstrual-storage");
                        saveMenstrualPeriodCareTrigger({ characterId: periodCare.characterId, sessionId, cycleKey: periodCare.cycleKey });
                    }

                    const idleMeta = (meta as { idleReconnect?: { ruleId?: string; firedAt?: number } }).idleReconnect;
                    if (idleMeta?.ruleId) {
                        const { markIdleReconnectFired } = await import("./idle-reconnect-storage");
                        markIdleReconnectFired(idleMeta.ruleId, typeof idleMeta.firedAt === "number" ? idleMeta.firedAt : Date.now());
                    }

                    const followUpIndex = typeof meta.followUpIndex === "number" ? meta.followUpIndex : undefined;
                    const existingMessages = loadChatMessages(sessionId);
                    if (followUpIndex && existingMessages.some(m => m.role === "assistant" && m.followUpIndex === followUpIndex)) {
                        clearTimedWakeIfHandled(entry.trigger_key);
                        consumedIds.push(entry.id);
                        if (entry.trigger_key) handledTriggerKeys.add(entry.trigger_key);
                        continue;
                    }

                    const armAtMs = typeof meta.armAt === "string" ? Date.parse(meta.armAt) : NaN;
                    // 基线快照：本轮开始前该会话已有的消息 id。合并进来的消息
                    // 现在带服务端生成时间（早于 passStartMs），不排除基线外
                    // 的新消息就会被下面的时间窗判定误认成「本地已生成」。
                    if (!passBaselineIds.has(sessionId)) {
                        passBaselineIds.set(sessionId, new Set(existingMessages.map(m => m.id)));
                    }
                    const baselineIds = passBaselineIds.get(sessionId)!;
                    if (Number.isFinite(armAtMs) && existingMessages.some(m => {
                        if (m.role !== "assistant") return false;
                        if (!baselineIds.has(m.id)) return false; // 本轮刚合并进来的，不算本地产物
                        const createdMs = Date.parse(m.createdAt);
                        return createdMs > armAtMs && createdMs < passStartMs;
                    })) {
                        clearTimedWakeIfHandled(entry.trigger_key);
                        consumedIds.push(entry.id);
                        if (entry.trigger_key) handledTriggerKeys.add(entry.trigger_key);
                        continue;
                    }

                    let text = stripHallucinatedTimestamps(entry.raw_text.trim());
                    const regexes = Array.isArray(meta.regexes) ? meta.regexes : [];
                    if (regexes.length > 0) {
                        const macroEngine = new MacroEngine(meta.characterName ?? "", meta.userName ?? "用户");
                        const activeTags = getActiveAppTags(meta.appId ?? "chat", { appTags: meta.appTags, followUpCount: meta.followUpCount });
                        text = applyOutputRegex(text, regexes, { macroEngine, activeTags });
                    }

                    // 云端执行过的快捷动作标记：在原始位置落一对 tool_call/tool_notice——
                    // 上下文里保留标记原文，UI 显示与小手机内直接调用一致
                    const rawMarker = (meta as { shortcutMarker?: { text?: unknown; insertAt?: unknown; name?: unknown } }).shortcutMarker;
                    const shortcutMarker = rawMarker
                        && typeof rawMarker.text === "string" && rawMarker.text
                        && typeof rawMarker.name === "string" && rawMarker.name
                        ? {
                            text: rawMarker.text,
                            insertAt: typeof rawMarker.insertAt === "number" && Number.isFinite(rawMarker.insertAt)
                                ? rawMarker.insertAt
                                : Number.MAX_SAFE_INTEGER,
                            name: rawMarker.name,
                        }
                        : undefined;
                    const { hasVisible, newCount, stateValues } = await parseAndSaveResponse(
                        text,
                        sessionId,
                        meta.prevCount ?? 0,
                        followUpIndex,
                        existingMessages,
                        {
                            silent: options?.silent !== false,
                            // 沿用服务端生成时间，而不是「合并那一刻」。
                            // 不传时 parseAndSaveResponse 落到当前时间，离线期间
                            // 积压的多条消息会全部挤在用户刚上线的同一时刻，
                            // 既看不出真实时间，顺序也被打乱。
                            // bridge / 屏幕速聊分支早就传了 createdAt，这里此前漏了。
                            ...(entry.created_at ? { createdAt: entry.created_at } : {}),
                            ...(shortcutMarker ? { shortcutMarker } : {}),
                        },
                    );
                    if (hasVisible) mergedSessionIds.add(sessionId);
                    if (hasVisible && newCount < 10) scheduleFollowUp(sessionId, newCount, stateValues);
                    clearTimedWakeIfHandled(entry.trigger_key);
                    consumedIds.push(entry.id);
                    if (entry.trigger_key) handledTriggerKeys.add(entry.trigger_key);
                } catch (err) {
                    console.warn("[PushOutbox] merge failed for entry:", entry.id, err);
                    // 屏幕速聊各轮有严格因果顺序；前一轮未合并时不能越过它消费后一轮。
                    if ((entry.meta as { screenChat?: boolean } | null)?.screenChat === true) break;
                }
            }

            if (consumedIds.length === 0) break;
            const ackInit: RequestInit = {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ ids: consumedIds }),
            };
            const ackResponse = await (source === "personal"
                ? personalPushFetch("outbox", ackInit)
                : fetch("/api/push/outbox", { ...ackInit, credentials: "include" }))
                .catch(() => null);
            if (!ackResponse || !ackResponse.ok) break;
            if (entries.length < OUTBOX_BATCH_SIZE) break;
          }
        }
        // 收尾：把本轮合并过消息的会话按时间重排一次显示顺序。
        // bridge / 屏幕速聊分支各自在分支内调过，这里统一兜住普通离线消息。
        if (mergedSessionIds.size > 0) {
            for (const sid of mergedSessionIds) reindexSessionMessageOrdersByTime(sid);
        }
    } finally {
        consuming = false;
    }
}

/** 安装自动消费钩子：启动后拉一次，之后每次回前台再拉。 */
export function installServerOutboxConsumer(): void {
    if (typeof window === "undefined" || consumerInstalled) return;
    consumerInstalled = true;

    const requestConsume = (force = false) => {
        if (consumeRequestTimer !== null) window.clearTimeout(consumeRequestTimer);
        consumeRequestTimer = window.setTimeout(() => {
            consumeRequestTimer = null;
            void consumeServerOutbox({ force });
        }, 150);
    };

    // 每次冷启动都强制拉一次（force=true 会绕过 consumeServerOutbox 内的 5 分钟节流）。
    //
    // 之前这里是非强制：用户若在 5 分钟内重开 App，启动拉取被节流跳过，聊天里就看不到
    // 服务端已经生成、通知也已经弹过的那条消息——「弹窗比聊天里的消息慢」正是这么来的。
    // 主动消息本来就该「打开 App 即见」，不该依赖用户去点那条通知，也不该被节流挡下。
    // 代价仅是多一次 outbox 查询；服务端返回空列表时客户端立刻结束。
    requestConsume(true);
    document.addEventListener("visibilitychange", () => {
        if (!document.hidden) requestConsume(true);
    });
    navigator.serviceWorker?.addEventListener("message", (event) => {
        if (event.data?.type === "push_outbox_ready") {
            requestConsume(true);
            return;
        }
        if (event.data?.type === "run_shortcut" && typeof event.data.url === "string") {
            const url: string = event.data.url;
            // 站点线的 /shortcut-run 票据地址、个人线的同源转发路由，
            // 或个人云网关自己的 run 入口
            const personal = loadPersonalPushCloudState();
            const allowed = url.startsWith(`${window.location.origin}/shortcut-run`)
                || url.startsWith(`${window.location.origin}/personal-shortcut-run?`)
                || (personal !== null && url.startsWith(`${personal.url}/functions/v1/ai-phone-push?action=run&`));
            if (allowed) window.location.href = url;
        }
    });
}
