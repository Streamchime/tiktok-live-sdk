// Generates src/types.ts from schema/event.v1.schema.json with json-schema-to-typescript.
// The output is committed: run `npm run build:types` after the schema copy changes and commit
// the result, rather than generating it as part of every install or test run.
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { compile } from "json-schema-to-typescript";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const schemaPath = path.join(root, "schema", "event.v1.schema.json");
const outPath = path.join(root, "src", "types.ts");

const schemaText = await readFile(schemaPath, "utf8");
const schema = JSON.parse(schemaText);

const banner = `/* eslint-disable */
/**
 * This file was generated from schema/event.v1.schema.json by scripts/types.mjs
 * (json-schema-to-typescript). Do not edit by hand; edit the schema copy and regenerate
 * with \`npm run build:types\`.
 */

`;

const body = await compile(schema, "Event", {
  bannerComment: "",
  style: { singleQuote: false },
  additionalProperties: false,
  // The schema's payload definitions (GiftPayload, MessagePayload, and so on) are not reached by
  // a $ref from the root Event type on purpose (payload is typed as a generic object, "selected by
  // type"), but the SDK still wants named types for them so TikTokRoom's per-event listeners can be
  // typed. unreachableDefinitions emits every named definition, not only the ones the root reaches.
  unreachableDefinitions: true,
});

await writeFile(outPath, banner + body, "utf8");
console.log(`wrote ${path.relative(root, outPath)}`);
