import mermaid from 'mermaid';
import { readFileSync } from 'node:fs';

const chart = readFileSync(new URL('./tmp-chart.mmd', import.meta.url), 'utf8');

try {
  const result = await mermaid.parse(chart);
  console.log('PARSE_OK', JSON.stringify(result));
} catch (e) {
  console.error('PARSE_FAIL');
  console.error('message:', e?.message);
  console.error('str:', e?.str);
  if (e?.hash) console.error('hash:', JSON.stringify(e.hash, null, 2));
  console.error(e);
  process.exitCode = 1;
}
