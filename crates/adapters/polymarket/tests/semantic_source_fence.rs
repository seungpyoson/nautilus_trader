// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use std::{path::Path, process::Command};

use rstest::rstest;

#[rstest]
fn semantic_generated_artifact_and_source_fence_are_exact() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("polymarket crate must remain under crates/adapters");

    let generator = Command::new("python3")
        .args([
            "scripts/generate_polymarket_semantic_boundary.py",
            "--check",
        ])
        .current_dir(repository)
        .status()
        .expect("generator check must launch");
    assert!(generator.success(), "generated semantic boundary drifted");

    let fence = Command::new("python3")
        .args([
            "scripts/verify_polymarket_semantic_boundary.py",
            "--check",
            "--self-test",
        ])
        .current_dir(repository)
        .status()
        .expect("semantic source fence must launch");
    assert!(fence.success(), "semantic source fence rejected the tree");
}
