// FILE: CodexTextGeneration.ts
// Purpose: Runs schema-constrained Codex CLI text generation against account-owned auth.
// Layer: Git and orchestration text-generation service.

import { randomUUID } from "node:crypto";

import { Effect, FileSystem, Layer, Option, Path, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { DEFAULT_GIT_TEXT_GENERATION_MODEL } from "@synara/contracts";
import { sanitizeGeneratedThreadTitle } from "@synara/shared/chatThreads";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@synara/shared/git";
import { prepareWindowsSafeProcess } from "@synara/shared/windowsProcess";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { buildCodexProcessLaunchContext } from "../../codexProcessEnv.ts";
import { ServerConfig } from "../../config.ts";
import { TextGenerationError } from "../Errors.ts";
import {
  CodexTextGeneration,
  type BranchNameGenerationInput,
  type BranchNameGenerationResult,
  type CommitMessageGenerationResult,
  type DiffSummaryGenerationResult,
  type PrContentGenerationResult,
  type ThreadTitleGenerationResult,
  type ThreadRecapGenerationResult,
  type TextGenerationOperation,
  type TextGenerationShape,
  TextGeneration,
} from "../Services/TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildAutomationIntentPrompt,
  buildAutomationCompletionEvaluationPrompt,
  buildCommitMessagePrompt,
  buildDiffSummaryPrompt,
  buildPrContentPrompt,
  buildThreadRecapPrompt,
  buildThreadTitlePrompt,
  sanitizeCommitSubject,
  sanitizeDiffSummary,
  sanitizeThreadRecap,
  sanitizePrTitle,
  toJsonSchemaObject,
} from "../textGenerationShared.ts";

const CODEX_REASONING_EFFORT = "low";
const CODEX_TIMEOUT_MS = 180_000;

function normalizeCodexError(
  binaryPath: string,
  operation: string,
  error: unknown,
  fallback: string,
): TextGenerationError {
  if (Schema.is(TextGenerationError)(error)) {
    return error;
  }

  if (error instanceof Error) {
    const lower = error.message.toLowerCase();
    if (
      error.message.includes(`Command not found: ${binaryPath}`) ||
      lower.includes(`spawn ${binaryPath.toLowerCase()}`) ||
      lower.includes("enoent")
    ) {
      return new TextGenerationError({
        operation,
        detail: `Codex CLI (${binaryPath}) is required but not available.`,
        cause: error,
      });
    }
    return new TextGenerationError({
      operation,
      detail: `${fallback}: ${error.message}`,
      cause: error,
    });
  }

  return new TextGenerationError({
    operation,
    detail: fallback,
    cause: error,
  });
}

const makeCodexTextGeneration = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* Effect.service(ServerConfig);

  type MaterializedImageAttachments = {
    readonly imagePaths: ReadonlyArray<string>;
  };

  const readStreamAsString = <E>(
    operation: string,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    Effect.gen(function* () {
      let text = "";
      yield* Stream.runForEach(stream, (chunk) =>
        Effect.sync(() => {
          text += Buffer.from(chunk).toString("utf8");
        }),
      ).pipe(
        Effect.mapError((cause) =>
          normalizeCodexError("codex", operation, cause, "Failed to collect process output"),
        ),
      );
      return text;
    });

  const tempDir = process.env.TMPDIR ?? process.env.TEMP ?? process.env.TMP ?? "/tmp";

  const writeTempFile = (
    operation: string,
    prefix: string,
    content: string,
  ): Effect.Effect<string, TextGenerationError> => {
    const filePath = path.join(tempDir, `synara-${prefix}-${process.pid}-${randomUUID()}.tmp`);
    return fileSystem.writeFileString(filePath, content).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: `Failed to write temp file at ${filePath}.`,
            cause,
          }),
      ),
      Effect.as(filePath),
    );
  };

  const safeUnlink = (filePath: string): Effect.Effect<void, never> =>
    fileSystem.remove(filePath).pipe(Effect.catch(() => Effect.void));

  const materializeImageAttachments = (
    _operation: TextGenerationOperation,
    attachments: BranchNameGenerationInput["attachments"],
  ): Effect.Effect<MaterializedImageAttachments, TextGenerationError> =>
    Effect.gen(function* () {
      if (!attachments || attachments.length === 0) {
        return { imagePaths: [] };
      }

      const imagePaths: string[] = [];
      for (const attachment of attachments) {
        if (attachment.type !== "image") {
          continue;
        }

        const resolvedPath = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!resolvedPath || !path.isAbsolute(resolvedPath)) {
          continue;
        }
        const fileInfo = yield* fileSystem
          .stat(resolvedPath)
          .pipe(Effect.catch(() => Effect.succeed(null)));
        if (!fileInfo || fileInfo.type !== "File") {
          continue;
        }
        imagePaths.push(resolvedPath);
      }
      return { imagePaths };
    });

  const runCodexJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    imagePaths = [],
    cleanupPaths = [],
    codexHomePath,
    model,
    modelSelection,
    providerOptions,
  }: {
    operation: TextGenerationOperation;
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    imagePaths?: ReadonlyArray<string>;
    cleanupPaths?: ReadonlyArray<string>;
    codexHomePath?: string;
    model?: string;
    modelSelection?: BranchNameGenerationInput["modelSelection"];
    providerOptions?: BranchNameGenerationInput["providerOptions"];
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const codexBinaryPath = resolveCodexBinaryPath(providerOptions);
      const resolvedCodexHomePath = resolveCodexHomePath(codexHomePath, providerOptions);
      const resolvedCodexAuthHomePath = resolveCodexAuthHomePath(providerOptions);
      const resolvedCodexAccountId = resolveCodexAccountId(providerOptions);
      const schemaPath = yield* writeTempFile(
        operation,
        "codex-schema",
        JSON.stringify(toJsonSchemaObject(outputSchemaJson)),
      );
      const outputPath = yield* writeTempFile(operation, "codex-output", "");
      const instanceLaunchEnv = providerOptions?.codex?.environment
        ? { ...process.env, ...providerOptions.codex.environment }
        : process.env;
      const processLaunch = yield* Effect.try({
        try: () =>
          buildCodexProcessLaunchContext({
            env: instanceLaunchEnv,
            ...(resolvedCodexHomePath ? { homePath: resolvedCodexHomePath } : {}),
            ...(resolvedCodexAuthHomePath ? { shadowHomePath: resolvedCodexAuthHomePath } : {}),
            ...(resolvedCodexAccountId ? { accountId: resolvedCodexAccountId } : {}),
          }),
        catch: (cause) =>
          new TextGenerationError({
            operation,
            detail:
              cause instanceof Error
                ? cause.message
                : "Codex authentication storage cannot be resolved safely.",
            cause,
          }),
      });

      const runCodexCommand = Effect.gen(function* () {
        // `--ignore-user-config` suppresses skills/plugins while explicitly
        // retaining CODEX_HOME auth. Point that home at the selected account's
        // authoritative file instead of cloning a rotating refresh token into
        // disposable state.
        const env = {
          ...processLaunch.env,
          CODEX_HOME: path.dirname(processLaunch.authTracking.authoritativeAuthFilePath),
        };
        const args = [
          "exec",
          "--ephemeral",
          "--ignore-user-config",
          "--skip-git-repo-check",
          "--config",
          'approval_policy="never"',
          "-s",
          "read-only",
          "--model",
          resolveCodexModel(model, modelSelection) ?? DEFAULT_GIT_TEXT_GENERATION_MODEL,
          "--config",
          `model_reasoning_effort="${CODEX_REASONING_EFFORT}"`,
          "--output-schema",
          schemaPath,
          "--output-last-message",
          outputPath,
          ...imagePaths.flatMap((imagePath) => ["--image", imagePath]),
          "-",
        ];
        const prepared = prepareWindowsSafeProcess(codexBinaryPath, args, { cwd, env });
        const command = ChildProcess.make(prepared.command, prepared.args, {
          cwd,
          env,
          shell: prepared.shell,
          stdin: {
            stream: Stream.make(new TextEncoder().encode(prompt)),
          },
        });

        const child = yield* commandSpawner
          .spawn(command)
          .pipe(
            Effect.mapError((cause) =>
              normalizeCodexError(
                codexBinaryPath,
                operation,
                cause,
                "Failed to spawn Codex CLI process",
              ),
            ),
          );

        const [stdout, stderr, exitCode] = yield* Effect.all(
          [
            readStreamAsString(operation, child.stdout),
            readStreamAsString(operation, child.stderr),
            child.exitCode.pipe(
              Effect.map((value) => Number(value)),
              Effect.mapError((cause) =>
                normalizeCodexError(
                  codexBinaryPath,
                  operation,
                  cause,
                  "Failed to read Codex CLI exit code",
                ),
              ),
            ),
          ],
          { concurrency: "unbounded" },
        );

        if (exitCode !== 0) {
          const stderrDetail = stderr.trim();
          const stdoutDetail = stdout.trim();
          const detail = stderrDetail.length > 0 ? stderrDetail : stdoutDetail;
          return yield* new TextGenerationError({
            operation,
            detail:
              detail.length > 0
                ? `Codex CLI command failed: ${detail}`
                : `Codex CLI command failed with code ${exitCode}.`,
          });
        }
      });

      const cleanup = Effect.all(
        [
          safeUnlink(schemaPath),
          safeUnlink(outputPath),
          ...cleanupPaths.map((filePath) => safeUnlink(filePath)),
        ],
        {
          concurrency: "unbounded",
        },
      ).pipe(Effect.asVoid);

      return yield* Effect.gen(function* () {
        yield* runCodexCommand.pipe(
          Effect.scoped,
          Effect.timeoutOption(CODEX_TIMEOUT_MS),
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new TextGenerationError({ operation, detail: "Codex CLI request timed out." }),
                ),
              onSome: () => Effect.void,
            }),
          ),
        );

        return yield* fileSystem.readFileString(outputPath).pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation,
                detail: "Failed to read Codex output file.",
                cause,
              }),
          ),
          Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson))),
          Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "Codex returned invalid structured output.",
                cause,
              }),
            ),
          ),
        );
      }).pipe(Effect.ensuring(cleanup));
    });

  const generateCommitMessage: TextGenerationShape["generateCommitMessage"] = (input) => {
    const wantsBranch = input.includeBranch === true;
    const { prompt, outputSchemaJson } = buildCommitMessagePrompt({
      branch: input.branch,
      stagedSummary: input.stagedSummary,
      stagedPatch: input.stagedPatch,
      includeBranch: wantsBranch,
    });

    return runCodexJson({
      operation: "generateCommitMessage",
      cwd: input.cwd,
      prompt,
      outputSchemaJson,
      ...(input.codexHomePath ? { codexHomePath: input.codexHomePath } : {}),
      ...(input.model ? { model: input.model } : {}),
      ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
      ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
    }).pipe(
      Effect.map(
        (generated) =>
          ({
            subject: sanitizeCommitSubject(generated.subject),
            body: generated.body.trim(),
            ...("branch" in generated && typeof generated.branch === "string"
              ? { branch: sanitizeFeatureBranchName(generated.branch) }
              : {}),
          }) satisfies CommitMessageGenerationResult,
      ),
    );
  };

  const generatePrContent: TextGenerationShape["generatePrContent"] = (input) => {
    const { prompt, outputSchemaJson } = buildPrContentPrompt({
      baseBranch: input.baseBranch,
      headBranch: input.headBranch,
      commitSummary: input.commitSummary,
      diffSummary: input.diffSummary,
      diffPatch: input.diffPatch,
    });

    return runCodexJson({
      operation: "generatePrContent",
      cwd: input.cwd,
      prompt,
      outputSchemaJson,
      ...(input.codexHomePath ? { codexHomePath: input.codexHomePath } : {}),
      ...(input.model ? { model: input.model } : {}),
      ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
      ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
    }).pipe(
      Effect.map(
        (generated) =>
          ({
            title: sanitizePrTitle(generated.title),
            body: generated.body.trim(),
          }) satisfies PrContentGenerationResult,
      ),
    );
  };

  const generateDiffSummary: TextGenerationShape["generateDiffSummary"] = (input) => {
    const { prompt, outputSchemaJson } = buildDiffSummaryPrompt({
      patch: input.patch,
    });

    return runCodexJson({
      operation: "generateDiffSummary",
      cwd: input.cwd,
      prompt,
      outputSchemaJson,
      ...(input.codexHomePath ? { codexHomePath: input.codexHomePath } : {}),
      ...(input.model ? { model: input.model } : {}),
      ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
      ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
    }).pipe(
      Effect.map(
        (generated) =>
          ({
            summary: sanitizeDiffSummary(generated.summary),
          }) satisfies DiffSummaryGenerationResult,
      ),
    );
  };

  const generateBranchName: TextGenerationShape["generateBranchName"] = (input) => {
    return Effect.gen(function* () {
      const { imagePaths } = yield* materializeImageAttachments(
        "generateBranchName",
        input.attachments,
      );
      const { prompt, outputSchemaJson } = buildBranchNamePrompt({
        message: input.message,
        ...(input.attachments ? { attachments: input.attachments } : {}),
      });

      const generated = yield* runCodexJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson,
        imagePaths,
        ...(input.model ? { model: input.model } : {}),
        ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
        ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      } satisfies BranchNameGenerationResult;
    });
  };

  const generateThreadTitle: TextGenerationShape["generateThreadTitle"] = (input) => {
    return Effect.gen(function* () {
      const { imagePaths } = yield* materializeImageAttachments(
        "generateThreadTitle",
        input.attachments,
      );
      const { prompt, outputSchemaJson } = buildThreadTitlePrompt({
        message: input.message,
        ...(input.attachments ? { attachments: input.attachments } : {}),
      });

      const generated = yield* runCodexJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson,
        imagePaths,
        ...(input.model ? { model: input.model } : {}),
        ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
        ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
      });

      return {
        title: sanitizeGeneratedThreadTitle(generated.title),
      } satisfies ThreadTitleGenerationResult;
    });
  };

  const generateThreadRecap: TextGenerationShape["generateThreadRecap"] = (input) => {
    const { prompt, outputSchemaJson } = buildThreadRecapPrompt({
      ...(input.previousRecap ? { previousRecap: input.previousRecap } : {}),
      newMaterial: input.newMaterial,
      ...(input.currentState ? { currentState: input.currentState } : {}),
    });

    return runCodexJson({
      operation: "generateThreadRecap",
      cwd: input.cwd,
      prompt,
      outputSchemaJson,
      ...(input.codexHomePath ? { codexHomePath: input.codexHomePath } : {}),
      ...(input.model ? { model: input.model } : {}),
      ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
      ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
    }).pipe(
      Effect.map(
        (generated) =>
          ({
            recap: sanitizeThreadRecap(generated.recap, input.previousRecap),
          }) satisfies ThreadRecapGenerationResult,
      ),
    );
  };

  const generateAutomationIntent: TextGenerationShape["generateAutomationIntent"] = (input) => {
    const { prompt, outputSchemaJson } = buildAutomationIntentPrompt({
      message: input.message,
      ...(input.defaultMode ? { defaultMode: input.defaultMode } : {}),
      nowIso: input.nowIso,
    });

    return runCodexJson({
      operation: "generateAutomationIntent",
      cwd: input.cwd,
      prompt,
      outputSchemaJson,
      ...(input.codexHomePath ? { codexHomePath: input.codexHomePath } : {}),
      ...(input.model ? { model: input.model } : {}),
      ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
      ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
    });
  };

  const evaluateAutomationCompletion: TextGenerationShape["evaluateAutomationCompletion"] = (
    input,
  ) => {
    const { prompt, outputSchemaJson } = buildAutomationCompletionEvaluationPrompt(input);

    return runCodexJson({
      operation: "evaluateAutomationCompletion",
      cwd: input.cwd,
      prompt,
      outputSchemaJson,
      ...(input.codexHomePath ? { codexHomePath: input.codexHomePath } : {}),
      ...(input.model ? { model: input.model } : {}),
      ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
      ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
    });
  };

  return {
    generateCommitMessage,
    generatePrContent,
    generateDiffSummary,
    generateBranchName,
    generateThreadTitle,
    generateThreadRecap,
    generateAutomationIntent,
    evaluateAutomationCompletion,
  } satisfies TextGenerationShape;
});

function resolveCodexBinaryPath(
  providerOptions: BranchNameGenerationInput["providerOptions"] | undefined,
): string {
  return providerOptions?.codex?.binaryPath?.trim() || "codex";
}

function resolveCodexHomePath(
  codexHomePath: string | undefined,
  providerOptions: BranchNameGenerationInput["providerOptions"] | undefined,
): string | undefined {
  // The routed instance home wins: the legacy top-level codexHomePath is the
  // global default and must not override a selected account's own home.
  const resolved = providerOptions?.codex?.homePath?.trim() || codexHomePath?.trim();
  return resolved && resolved.length > 0 ? resolved : undefined;
}

function resolveCodexAuthHomePath(
  providerOptions: BranchNameGenerationInput["providerOptions"] | undefined,
): string | undefined {
  const resolved = providerOptions?.codex?.shadowHomePath?.trim();
  return resolved && resolved.length > 0 ? resolved : undefined;
}

function resolveCodexAccountId(
  providerOptions: BranchNameGenerationInput["providerOptions"] | undefined,
): string | undefined {
  const resolved = providerOptions?.codex?.accountId?.trim();
  return resolved && resolved.length > 0 ? resolved : undefined;
}

function resolveCodexModel(
  model: string | undefined,
  modelSelection: BranchNameGenerationInput["modelSelection"] | undefined,
): string | undefined {
  return modelSelection?.model ?? model;
}

export const CodexTextGenerationServiceLive = Layer.effect(
  CodexTextGeneration,
  makeCodexTextGeneration,
);

export const CodexTextGenerationLive = Layer.effect(TextGeneration, makeCodexTextGeneration);
