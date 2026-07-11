// FILE: composerStackedHeaderFrame.test.ts
// Purpose: Pins the shared composer-stacked activity rail token used by ComposerStackedHeaderFrame.
// Layer: Chat composer regression test
// Depends on: composerPickerStyles sizing token.

import { describe, expect, it } from "vitest";

import { COMPOSER_STACKED_HEADER_FRAME_CLASS_NAME } from "./composerPickerStyles";

describe("COMPOSER_STACKED_HEADER_FRAME_CLASS_NAME", () => {
  it("spans the composer width while staying centered above the input", () => {
    const classes = COMPOSER_STACKED_HEADER_FRAME_CLASS_NAME.split(/\s+/);

    expect(classes).toContain("-mb-px");
    expect(classes).toContain("w-full");
    expect(classes).toContain("min-w-0");
    // The full-width rail must stay centered so it remains aligned with the
    // composer input rather than hugging one edge.
    expect(classes).toContain("mx-auto");
  });
});
