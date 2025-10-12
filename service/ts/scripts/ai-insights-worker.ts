import 'dotenv/config';
import { randomUUID } from 'crypto';
import { setTimeout as sleep } from 'timers/promises';
import OpenAI from 'openai';

import {
  getStore,
  type PlayerInsightsQuery,
  type PlayerInsightAiJob,
  type PlayerInsightAiJobCompletion,
  type PlayerInsightAiStatus,
} from '../src/store/index.js';
import { buildAiPrompt } from '../src/insights/ai-prompt.js';

const WORKER_ID = process.env.AI_INSIGHTS_WORKER_ID ?? randomUUID();
const POLL_INTERVAL_MS = Number(process.env.AI_INSIGHTS_WORKER_POLL_MS ?? '1500');
const LOOP_COOLDOWN_MS = Number(process.env.AI_INSIGHTS_LOOP_COOLDOWN_MS ?? '200');
const FAILURE_BACKOFF_MS = Number(process.env.AI_INSIGHTS_FAILURE_BACKOFF_MS ?? '60000');
const QUOTA_BACKOFF_MS = Number(process.env.AI_INSIGHTS_QUOTA_BACKOFF_MS ?? '300000');
const TTL_HOURS = Number(process.env.AI_INSIGHTS_TTL_HOURS ?? '24');
const MODEL = process.env.AI_INSIGHTS_MODEL ?? 'gpt-4o-mini';
const RAW_TEMPERATURE = process.env.AI_INSIGHTS_TEMPERATURE;
const TEMPERATURE = RAW_TEMPERATURE !== undefined ? Number(RAW_TEMPERATURE) : null;
const MAX_OUTPUT_TOKENS = Number(process.env.AI_INSIGHTS_MAX_OUTPUT_TOKENS ?? '400');
const OPENAI_BASE_URL = process.env.OPENAI_API_BASE;

const normalizeReasoningEffort = (value: string | null | undefined) => {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  return normalized === 'low' || normalized === 'medium' || normalized === 'high' ? normalized : null;
};

const normalizeReasoningSummary = (value: string | null | undefined) => {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  return normalized === 'auto' || normalized === 'concise' || normalized === 'detailed' ? normalized : null;
};

const DEFAULT_REASONING_EFFORT = normalizeReasoningEffort(process.env.AI_INSIGHTS_REASONING_EFFORT) ?? 'medium';
const DEFAULT_REASONING_SUMMARY = normalizeReasoningSummary(process.env.AI_INSIGHTS_REASONING_SUMMARY) ?? 'auto';
const MIN_REASONING_OUTPUT_TOKENS = Number(process.env.AI_INSIGHTS_REASONING_MIN_OUTPUT_TOKENS ?? '600');

const isReasoningModel = (model: string) => {
  const normalized = model.toLowerCase();
  if (normalized.startsWith('gpt-5')) return true;
  if (normalized.startsWith('o1')) return true;
  if (normalized.startsWith('o2')) return true;
  if (normalized.startsWith('o3')) return true;
  if (normalized.startsWith('o4')) return true;
  return normalized.includes('-reasoning');
};

const apiKey = process.env.OPENAI_API_KEY;
if (!apiKey) {
  console.error('ai_insights_worker_missing_api_key');
  process.exit(1);
}

const openai = new OpenAI({ apiKey, baseURL: OPENAI_BASE_URL });
const store = getStore();

const computeExpiry = (hours: number) => {
  if (!Number.isFinite(hours) || hours <= 0) return null;
  return new Date(Date.now() + hours * 60 * 60 * 1000);
};

const safeSleep = async (ms: number) => {
  try {
    await sleep(ms);
  } catch (err) {
    if ((err as any)?.name !== 'AbortError') {
      throw err;
    }
  }
};

const resolveMaxOutputTokens = () => {
  const fallback = 400;
  const configured = Number.isFinite(MAX_OUTPUT_TOKENS) && MAX_OUTPUT_TOKENS > 0 ? MAX_OUTPUT_TOKENS : fallback;
  if (!isReasoningModel(MODEL)) {
    return configured;
  }

  const minReasoning = Number.isFinite(MIN_REASONING_OUTPUT_TOKENS) && MIN_REASONING_OUTPUT_TOKENS > 0
    ? MIN_REASONING_OUTPUT_TOKENS
    : 600;

  return Math.max(configured, minReasoning);
};

const jobToQuery = (job: PlayerInsightAiJob): PlayerInsightsQuery => ({
  organizationId: job.organizationId,
  playerId: job.playerId,
  sport: job.sport ?? null,
  discipline: job.discipline ?? null,
});

const recordResult = async (
  job: PlayerInsightAiJob,
  status: PlayerInsightAiStatus,
  data: {
    narrative?: string | null;
    model?: string | null;
    errorCode?: string | null;
    errorMessage?: string | null;
    tokens?: { prompt: number; completion: number; total: number } | null;
    generatedAt?: Date | string | null;
    expiresAt?: Date | string | null;
  }
) => {
  await store.savePlayerInsightAiResult({
    organizationId: job.organizationId,
    playerId: job.playerId,
    sport: job.sport ?? null,
    discipline: job.discipline ?? null,
    snapshotDigest: job.snapshotDigest,
    promptVersion: job.promptVersion,
    status,
    narrative: data.narrative ?? null,
    model: data.model ?? null,
    generatedAt: data.generatedAt ?? null,
    tokens: data.tokens ?? null,
    errorCode: data.errorCode ?? null,
    errorMessage: data.errorMessage ?? null,
    expiresAt: data.expiresAt ?? null,
  });
};

const handleJob = async (job: PlayerInsightAiJob) => {
  const completion: PlayerInsightAiJobCompletion = {
    jobId: job.jobId,
    workerId: WORKER_ID,
    success: false,
  };

  try {
    const query = jobToQuery(job);

    let snapshot = await store.getPlayerInsights(query);
    if (!snapshot || snapshot.cacheKeys?.digest !== job.snapshotDigest) {
      const rebuilt = await store.buildPlayerInsightsSnapshot(query);
      const upserted = await store.upsertPlayerInsightsSnapshot(query, rebuilt);
      snapshot = upserted.snapshot;
    }

    if (!snapshot || snapshot.cacheKeys?.digest !== job.snapshotDigest) {
      await recordResult(job, 'FAILED', {
        narrative: null,
        model: null,
        errorCode: 'snapshot_digest_mismatch',
        errorMessage: 'Insight snapshot digest no longer matches this job.',
        generatedAt: null,
        expiresAt: null,
        tokens: null,
      });
      completion.success = true;
      await store.completePlayerInsightAiJob(completion);
      return;
    }

    const player = await store.getPlayer(job.playerId, job.organizationId);
    const playerName = player?.displayName ?? player?.shortName ?? null;

    const { system, user } = buildAiPrompt({ snapshot, playerName });

    const request: Parameters<typeof openai.responses.create>[0] = {
      model: MODEL,
      max_output_tokens: resolveMaxOutputTokens(),
      text: { format: { type: 'text' } },
      input: [
        { role: 'system', content: system },
        { role: 'user', content: user },
      ],
    };

    if (TEMPERATURE !== null && Number.isFinite(TEMPERATURE)) {
      request.temperature = TEMPERATURE;
    }

    if (isReasoningModel(MODEL)) {
      request.reasoning = {
        effort: DEFAULT_REASONING_EFFORT,
        summary: DEFAULT_REASONING_SUMMARY,
      };
    }

    const response = await openai.responses.create(request);

    const extractNarrative = () => {
      const direct = typeof response.output_text === 'string' ? response.output_text.trim() : '';
      if (direct) return direct;

      const chunks: string[] = [];
      const pushText = (value: unknown) => {
        if (typeof value === 'string' && value.trim().length) {
          chunks.push(value.trim());
        }
        if (Array.isArray(value)) {
          for (const entry of value) {
            pushText(entry);
          }
        }
      };

      const visitContent = (content: any) => {
        if (!content) return;
        const type = content.type ?? null;
        if (type === 'text' || type === 'output_text') {
          pushText(content.text?.value ?? content.text ?? content.output_text);
        } else if (type === 'message') {
          for (const part of content.content ?? []) {
            visitContent(part);
          }
        } else if (typeof content === 'string') {
          pushText(content);
        } else if (Array.isArray(content)) {
          for (const entry of content) {
            visitContent(entry);
          }
        }
      };

      const outputs = (response as any)?.output ?? [];
      for (const message of outputs) {
        if (!message) continue;
        if (Array.isArray(message.content)) {
          for (const part of message.content) {
            visitContent(part);
          }
        } else {
          visitContent(message);
        }
      }

      if (!chunks.length) {
        const reasoningSummaries = outputs
          .filter((item: any) => item?.type === 'reasoning')
          .flatMap((item: any) =>
            Array.isArray(item?.summary)
              ? item.summary
                  .map((entry: any) => (typeof entry?.text === 'string' ? entry.text.trim() : ''))
                  .filter((entry: string) => entry.length > 0)
              : []
          );

        if (reasoningSummaries.length) {
          const summaryText = reasoningSummaries.join('\n').trim();
          if (summaryText) {
            console.info('ai_insights_narrative_from_reasoning_summary', {
              jobId: job.jobId,
              model: MODEL,
            });
            return summaryText;
          }
        }
      }

      if (!chunks.length) {
        const messages = (response as any)?.output?.map((entry: any) => ({
          type: entry?.type ?? null,
          contentTypes: Array.isArray(entry?.content)
            ? entry.content.map((part: any) => part?.type ?? typeof part ?? null)
            : entry?.content ?? null,
        })) ?? null;
        console.warn('ai_insights_empty_narrative_response', {
          jobId: job.jobId,
          model: MODEL,
          outputSummary: messages,
          rawOutput: (response as any)?.output ?? null,
        });
        return '';
      }

      return chunks.join('\n').trim();
    };

    const narrative = extractNarrative();
    if (!narrative) {
      throw new Error('empty_narrative');
    }

    const usage = response.usage ?? {};
    const tokens = usage.input_tokens !== undefined || usage.output_tokens !== undefined || usage.total_tokens !== undefined
      ? {
          prompt: usage.input_tokens ?? 0,
          completion: usage.output_tokens ?? 0,
          total:
            usage.total_tokens ??
            (usage.input_tokens ?? 0) + (usage.output_tokens ?? 0),
        }
      : null;

    const generatedAt = new Date();
    const expiresAt = computeExpiry(TTL_HOURS);

    await recordResult(job, 'READY', {
      narrative,
      model: MODEL,
      generatedAt,
      tokens,
      expiresAt,
      errorCode: null,
      errorMessage: null,
    });

    console.info('ai_insights_job_ready', {
      jobId: job.jobId,
      playerId: job.playerId,
      organizationId: job.organizationId,
      model: MODEL,
      narrativeLength: narrative.length,
      tokens,
    });

    completion.success = true;
  } catch (rawError) {
    const err = rawError as Error;
    console.error('ai_insights_job_error', {
      jobId: job.jobId,
      playerId: job.playerId,
      organizationId: job.organizationId,
      error: err,
    });

    let status: PlayerInsightAiStatus = 'FAILED';
    let errorCode = 'generation_error';
    let errorMessage = err.message ?? 'unknown_error';
    let rescheduleAt: Date | null | undefined = new Date(Date.now() + FAILURE_BACKOFF_MS);

    if (err instanceof OpenAI.APIError) {
      const statusCode = err.status ?? 0;
      errorMessage = err.error?.message ?? err.message;
      if (statusCode === 401 || statusCode === 403) {
        errorCode = 'auth_error';
        rescheduleAt = null;
      } else if (statusCode === 429 || statusCode === 503) {
        status = 'QUOTA_EXCEEDED';
        errorCode = 'rate_limited';
        rescheduleAt = new Date(Date.now() + QUOTA_BACKOFF_MS);
      } else if (statusCode >= 500) {
        errorCode = 'openai_service_error';
      }
    }

    await recordResult(job, status, {
      narrative: null,
      model: null,
      errorCode,
      errorMessage,
      generatedAt: null,
      expiresAt: null,
      tokens: null,
    });

    completion.error = errorMessage;
    completion.rescheduleAt = rescheduleAt ?? null;
  }

  await store.completePlayerInsightAiJob(completion);
};

const main = async () => {
  console.log('ai_insights_worker_started', { workerId: WORKER_ID, model: MODEL });

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const job = await store.claimPlayerInsightAiJob({ workerId: WORKER_ID });
    if (!job) {
      await safeSleep(POLL_INTERVAL_MS);
      continue;
    }

    await handleJob(job);
    await safeSleep(LOOP_COOLDOWN_MS);
  }
};

main().catch((err) => {
  console.error('ai_insights_worker_fatal', err);
  process.exitCode = 1;
});
