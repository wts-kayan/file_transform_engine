package com.bnp.str

/**
 * The `addons` engine/module — a sibling of [[com.bnp.str.tseadfwd]] under `com.bnp.str`.
 *
 * Put the addons code under this package (`com.bnp.str.addons.*`): its own jobs (objects with a
 * `main`, e.g. `com.bnp.str.addons.job.MainDriver`), its own config block, reusing the shared
 * `sessionmanager` / `reader` / `writer` / `utility` plumbing as needed. Everything still builds
 * into the one fat jar; pick the job to run with `spark-submit --class …`.
 *
 * This placeholder package object only exists so the package/folder is tracked in git; replace or
 * extend it freely as the real code lands.
 */
package object addons
