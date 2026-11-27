'use strict';
const { httpFailure, networkFailure } = require('../../services/ops-console-react/agent');
async function listModels(key, fetcher = fetch) {
  if (!key) throw new Error('GEMINI_API_KEY is required; load it in this terminal first');
  const names = new Set();
  let pageToken;
  for (let page = 0; page < 20; page++) {
    const url = new URL('https://generativelanguage.googleapis.com/v1beta/models');
    url.searchParams.set('pageSize', '100');
    if (pageToken) url.searchParams.set('pageToken', pageToken);
    let response;
    try {
      response = await fetcher(url.toString(), { headers: { 'x-goog-api-key': key },
        redirect: 'error', signal: AbortSignal.timeout(15000) });
    } catch (error) { throw new Error(JSON.stringify(networkFailure(error))); }
    if (!response.ok) throw new Error(JSON.stringify(await httpFailure(response)));
    let json;
    try { json = await response.json(); } catch { throw new Error('Invalid model-list response'); }
    if (!Array.isArray(json?.models)) throw new Error('Invalid model-list response');
    for (const model of json.models) {
      if (/^models\/gemini-[a-z0-9.-]+$/.test(model?.name) &&
          model.supportedGenerationMethods?.includes('generateContent')) names.add(model.name.slice(7));
    }
    if (!json.nextPageToken) return [...names].sort();
    pageToken = json.nextPageToken;
  }
  throw new Error('Model-list pagination limit reached');
}
if (require.main === module) listModels(process.env.GEMINI_API_KEY)
  .then(models => console.log(JSON.stringify({ models, note: 'Model visibility does not guarantee free-tier quota or structured-output support.' }, null, 2)))
  .catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { listModels };
