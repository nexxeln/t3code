import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import * as OpenCode2AdapterV2Testkit from "../orchestration-v2/Adapters/OpenCode2AdapterV2.testkit.ts";
import * as OpenCode2Server from "../provider/opencode2/OpenCode2Server.ts";
import * as OpenCode2TextGeneration from "./OpenCode2TextGeneration.ts";
import { OPENCODE2_TITLE_GENERATION } from "./OpenCode2TextGeneration.fixture.ts";

const layer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-opencode2-text-generation-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

it.layer(layer)("OpenCode2TextGeneration", (it) => {
  it.effect("generates a title in a temporary session on the free tier and removes it", () =>
    Effect.gen(function* () {
      const server = yield* OpenCode2AdapterV2Testkit.replayServer({
        provider: "opencode",
        protocol: OpenCode2AdapterV2Testkit.OPENCODE2_HTTP_PROTOCOL,
        version: "2.0.18",
        scenario: "opencode2_title_generation",
        entries: [...OPENCODE2_TITLE_GENERATION, { type: "runtime_exit", status: "success" }],
      });
      const textGeneration = yield* OpenCode2TextGeneration.make().pipe(
        Effect.provideService(OpenCode2Server.OpenCode2Server, server),
      );
      const title = yield* textGeneration.generateThreadTitle({
        cwd: process.cwd(),
        message: "fix the login redirect loop after oauth",
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode"),
          model: "opencode/big-pickle",
        },
      });
      assert.equal(title.title, "Fix OAuth Login Redirect Loop");
    }).pipe(Effect.scoped),
  );

  it.effect("generates at the model's default in place of a variant it does not list", () =>
    Effect.gen(function* () {
      const [subscribe, connected, , ...rest] = OPENCODE2_TITLE_GENERATION;
      const server = yield* OpenCode2AdapterV2Testkit.replayServer({
        provider: "opencode",
        protocol: OpenCode2AdapterV2Testkit.OPENCODE2_HTTP_PROTOCOL,
        version: "2.0.18",
        scenario: "opencode2_title_generation_unlisted_variant",
        entries: [
          {
            type: "expect_outbound",
            frame: { type: "model.list", input: { "location[directory]": process.cwd() } },
          },
          {
            type: "emit_inbound",
            frame: {
              type: "sdk.response",
              operation: "model.list",
              data: {
                location: { directory: process.cwd() },
                data: [
                  {
                    id: "deepseek-v4.1-flash",
                    modelID: "deepseek-v4.1-flash",
                    providerID: "opencode-go",
                    name: "DeepSeek V4.1 Flash",
                    package: "@opencode/ai/providers/openai-compatible",
                    capabilities: { tools: true, input: ["text"], output: ["text"] },
                    variants: ["low", "high", "max"].map((id) => ({
                      id,
                      settings: { reasoningEffort: id },
                    })),
                    time: { released: 1790812800000 },
                    cost: [{ input: 0, output: 0, cache: { read: 0, write: 0 } }],
                    status: "active",
                    enabled: true,
                    limit: { context: 1048576, output: 131072 },
                  },
                ],
              },
            },
          },
          subscribe!,
          connected!,
          {
            type: "expect_outbound",
            frame: {
              type: "session.create",
              input: {
                title: "T3 Code generateThreadTitle",
                location: { directory: "<any>" },
                model: { providerID: "opencode-go", id: "deepseek-v4.1-flash", variant: "high" },
                permissions: [{ action: "*", resource: "*", effect: "ask" }],
              },
            },
          },
          ...rest,
          { type: "runtime_exit", status: "success" },
        ],
      });
      const textGeneration = yield* OpenCode2TextGeneration.make().pipe(
        Effect.provideService(OpenCode2Server.OpenCode2Server, server),
      );
      const title = yield* textGeneration.generateThreadTitle({
        cwd: process.cwd(),
        message: "fix the login redirect loop after oauth",
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode"),
          model: "opencode-go/deepseek-v4.1-flash",
          options: [{ id: "variant", value: "medium" }],
        },
      });
      assert.equal(title.title, "Fix OAuth Login Redirect Loop");
    }).pipe(Effect.scoped),
  );

  it.effect("fails at once when the event stream drops before the reply", () =>
    Effect.gen(function* () {
      const [subscribe, connected, create, created] = OPENCODE2_TITLE_GENERATION;
      const sessionId = "ses_f0ec85cf6ffebfalpvV2E9nocu";
      const server = yield* OpenCode2AdapterV2Testkit.replayServer({
        provider: "opencode",
        protocol: OpenCode2AdapterV2Testkit.OPENCODE2_HTTP_PROTOCOL,
        version: "2.0.18",
        scenario: "opencode2_title_generation_stream_lost",
        entries: [
          subscribe!,
          connected!,
          create!,
          created!,
          {
            type: "expect_outbound",
            frame: { type: "session.prompt", input: { sessionID: sessionId, text: "<any>" } },
          },
          {
            type: "emit_inbound",
            frame: {
              type: "sdk.response",
              operation: "session.prompt",
              data: {
                data: {
                  id: "msg_1",
                  sessionID: sessionId,
                  time: { created: 1 },
                  type: "user",
                  payload: { text: "t" },
                  delivery: "steer",
                },
              },
            },
          },
          // The server goes away before the reply; the temporary session is still removed.
          { type: "runtime_exit", status: "success" },
          {
            type: "expect_outbound",
            frame: { type: "session.remove", input: { sessionID: sessionId } },
          },
          {
            type: "emit_inbound",
            frame: { type: "sdk.response", operation: "session.remove", data: null },
          },
        ],
      });
      const textGeneration = yield* OpenCode2TextGeneration.make().pipe(
        Effect.provideService(OpenCode2Server.OpenCode2Server, server),
      );
      const failure = yield* textGeneration
        .generateThreadTitle({
          cwd: process.cwd(),
          message: "fix the login redirect loop after oauth",
          modelSelection: {
            instanceId: ProviderInstanceId.make("opencode"),
            model: "opencode/big-pickle",
          },
        })
        .pipe(Effect.flip, Effect.timeout("10 seconds"));
      assert.equal(failure._tag, "TextGenerationError");
    }).pipe(Effect.scoped),
  );

  it.effect("keeps a provider's own error message out of the caller-visible detail", () =>
    Effect.gen(function* () {
      const [subscribe, connected, create, created] = OPENCODE2_TITLE_GENERATION;
      const sessionId = "ses_f0ec85cf6ffebfalpvV2E9nocu";
      // The spike's `text_generation` recording: a deny-all session is refused this way.
      const refusal = {
        type: "provider.auth",
        message:
          "Error from provider (Console): OpenCode's free tier can only be used from within OpenCode",
        status: 403,
      };
      const server = yield* OpenCode2AdapterV2Testkit.replayServer({
        provider: "opencode",
        protocol: OpenCode2AdapterV2Testkit.OPENCODE2_HTTP_PROTOCOL,
        version: "2.0.18",
        scenario: "opencode2_title_generation_refused",
        entries: [
          subscribe!,
          connected!,
          create!,
          created!,
          {
            type: "expect_outbound",
            frame: { type: "session.prompt", input: { sessionID: sessionId, text: "<any>" } },
          },
          {
            type: "emit_inbound",
            frame: {
              type: "sdk.response",
              operation: "session.prompt",
              data: {
                data: {
                  id: "msg_1",
                  sessionID: sessionId,
                  time: { created: 1 },
                  type: "user",
                  payload: { text: "t" },
                  delivery: "steer",
                },
              },
            },
          },
          {
            type: "emit_inbound",
            frame: {
              type: "sdk.event",
              event: {
                id: "evt_0eb7f9ae0001VCNDHVLnqTfoPG",
                created: 1790657403616,
                type: "session.execution.failed",
                data: { sessionID: sessionId, error: refusal },
                durable: { aggregateID: sessionId, seq: 7, version: 1 },
              },
            },
          },
          {
            type: "expect_outbound",
            frame: { type: "session.remove", input: { sessionID: sessionId } },
          },
          {
            type: "emit_inbound",
            frame: { type: "sdk.response", operation: "session.remove", data: null },
          },
          { type: "runtime_exit", status: "success" },
        ],
      });
      const textGeneration = yield* OpenCode2TextGeneration.make().pipe(
        Effect.provideService(OpenCode2Server.OpenCode2Server, server),
      );
      const failure = yield* textGeneration
        .generateThreadTitle({
          cwd: process.cwd(),
          message: "fix the login redirect loop after oauth",
          modelSelection: {
            instanceId: ProviderInstanceId.make("opencode"),
            model: "opencode/big-pickle",
          },
        })
        .pipe(Effect.flip);
      assert.equal(failure.detail, "OpenCode could not generate the text.");
      assert.deepEqual(failure.cause, refusal);
    }).pipe(Effect.scoped),
  );

  it.effect("ends at once on an execution end this build cannot decode", () =>
    Effect.gen(function* () {
      const [subscribe, connected, create, created] = OPENCODE2_TITLE_GENERATION;
      const sessionId = "ses_f0ec85cf6ffebfalpvV2E9nocu";
      const server = yield* OpenCode2AdapterV2Testkit.replayServer({
        provider: "opencode",
        protocol: OpenCode2AdapterV2Testkit.OPENCODE2_HTTP_PROTOCOL,
        version: "2.0.18",
        scenario: "opencode2_title_generation_unreadable_end",
        entries: [
          subscribe!,
          connected!,
          create!,
          created!,
          {
            type: "expect_outbound",
            frame: { type: "session.prompt", input: { sessionID: sessionId, text: "<any>" } },
          },
          {
            type: "emit_inbound",
            frame: {
              type: "sdk.response",
              operation: "session.prompt",
              data: {
                data: {
                  id: "msg_1",
                  sessionID: sessionId,
                  time: { created: 1 },
                  type: "user",
                  payload: { text: "t" },
                  delivery: "steer",
                },
              },
            },
          },
          // A reason added after 2.0.18: the full schema rejects the frame.
          {
            type: "emit_inbound",
            frame: {
              type: "sdk.event",
              event: {
                id: "evt_0eb7f9ae0001VCNDHVLnqTfoPG",
                created: 1790657403616,
                type: "session.execution.interrupted",
                data: { sessionID: sessionId, reason: "budget" },
                durable: { aggregateID: sessionId, seq: 7, version: 1 },
              },
            },
          },
          {
            type: "expect_outbound",
            frame: { type: "session.remove", input: { sessionID: sessionId } },
          },
          {
            type: "emit_inbound",
            frame: { type: "sdk.response", operation: "session.remove", data: null },
          },
          { type: "runtime_exit", status: "success" },
        ],
      });
      const textGeneration = yield* OpenCode2TextGeneration.make().pipe(
        Effect.provideService(OpenCode2Server.OpenCode2Server, server),
      );
      const failure = yield* textGeneration
        .generateThreadTitle({
          cwd: process.cwd(),
          message: "fix the login redirect loop after oauth",
          modelSelection: {
            instanceId: ProviderInstanceId.make("opencode"),
            model: "opencode/big-pickle",
          },
        })
        .pipe(Effect.flip, Effect.timeout("10 seconds"));
      assert.equal(failure._tag, "TextGenerationError");
    }).pipe(Effect.scoped),
  );
});
