import type {
  ExternalThreadMatchKind,
  OrchestrationProjectShell,
  ProjectId,
} from "@synara/contracts";
import { workspaceRootsEqual } from "@synara/shared/threadWorkspace";
import type { FileSystem, Path } from "effect";
import { Effect } from "effect";

import { parseManagedWorktreeWorkspaceRoot } from "../workspace/managedWorktree";

export interface ExternalThreadWorkspaceMatch {
  readonly projectId: ProjectId | null;
  readonly matchKind: ExternalThreadMatchKind;
}

function containingProjectMatches(input: {
  readonly cwd: string;
  readonly path: Path.Path;
  readonly platform: NodeJS.Platform;
  readonly projects: ReadonlyArray<OrchestrationProjectShell>;
}): ReadonlyArray<OrchestrationProjectShell> {
  return input.projects
    .filter((project) => {
      if (
        workspaceRootsEqual(project.workspaceRoot, input.cwd, { platform: input.platform })
      ) {
        return true;
      }
      const relative = input.path.relative(project.workspaceRoot, input.cwd);
      return (
        relative.length > 0 &&
        !relative.startsWith("..") &&
        !input.path.isAbsolute(relative)
      );
    })
    .toSorted((left, right) => right.workspaceRoot.length - left.workspaceRoot.length);
}

export function resolveExternalThreadWorkspaceMatch(input: {
  readonly cwd: string;
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly platform: NodeJS.Platform;
  readonly projects: ReadonlyArray<OrchestrationProjectShell>;
}): Effect.Effect<ExternalThreadWorkspaceMatch> {
  return Effect.gen(function* () {
    const cwd = input.path.resolve(input.cwd);
    const directMatches = containingProjectMatches({ ...input, cwd });
    const direct = directMatches[0];
    if (direct) {
      const equallySpecific = directMatches.filter(
        (project) => project.workspaceRoot.length === direct.workspaceRoot.length,
      );
      if (equallySpecific.length === 1) {
        return {
          projectId: direct.id,
          matchKind: workspaceRootsEqual(direct.workspaceRoot, cwd, {
            platform: input.platform,
          })
            ? "exact"
            : "descendant",
        } satisfies ExternalThreadWorkspaceMatch;
      }
      return { projectId: null, matchKind: "unmatched" } satisfies ExternalThreadWorkspaceMatch;
    }

    let currentPath = cwd;
    while (true) {
      const gitPointerFileContents = yield* input.fileSystem
        .readFileString(input.path.join(currentPath, ".git"))
        .pipe(Effect.catch(() => Effect.succeed(null)));
      if (gitPointerFileContents) {
        const workspaceRoot = parseManagedWorktreeWorkspaceRoot({
          gitPointerFileContents,
          path: input.path,
          worktreePath: currentPath,
        });
        if (workspaceRoot) {
          const owners = input.projects.filter((project) =>
            workspaceRootsEqual(project.workspaceRoot, workspaceRoot, {
              platform: input.platform,
            }),
          );
          if (owners.length === 1) {
            return {
              projectId: owners[0]!.id,
              matchKind: "managed-worktree",
            } satisfies ExternalThreadWorkspaceMatch;
          }
          return {
            projectId: null,
            matchKind: "unmatched",
          } satisfies ExternalThreadWorkspaceMatch;
        }
      }

      const parentPath = input.path.dirname(currentPath);
      if (parentPath === currentPath) {
        return { projectId: null, matchKind: "unmatched" } satisfies ExternalThreadWorkspaceMatch;
      }
      currentPath = parentPath;
    }
  });
}
