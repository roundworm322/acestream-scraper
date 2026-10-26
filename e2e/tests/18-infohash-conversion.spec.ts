import path from 'node:path';
import { test, expect } from '../src/fixtures';
import { startStubEngine, STUB_INFOHASH, STUB_INFOHASH_CONTENT_ID } from '../src/stub-engine';

const UNKNOWN_INFOHASH = 'e2e0000000000000000000000000000000000ba5';

test('an infohash added from search is stored as its content id', async ({ api, errors }) => {
  // The unknown-infohash request below is refused on purpose.
  errors.allowBackend(/APIError code=INFOHASH_UNRESOLVED/);
  const engineUrl = await api.getSetting('ace_engine_url');
  const stub = await startStubEngine(path.resolve(__dirname, '../../backend/tests/docker/fixtures/sample-h264-ac3.m2ts'),
    process.env.E2E_TARGET === 'docker' ? { bindHost: '0.0.0.0', advertisedHost: 'host.docker.internal' } : undefined);
  try {
    // The bug: an infohash asked for as a content id never plays.
    const direct = await fetch(`${stub.url}/ace/getstream?id=${STUB_INFOHASH}&format=json`).then(r => r.json());
    expect(direct).toEqual({ response: null, error: 'failed to load content' });

    await api.putSetting('ace_engine_url', stub.url);
    // Search → Play asks for the content ID before opening the player.
    const lookup = await api.raw('get', `/api/v1/search/${STUB_INFOHASH}/content_id`);
    expect(lookup.status()).toBe(200);
    expect(await lookup.json()).toEqual({ infohash: STUB_INFOHASH, content_id: STUB_INFOHASH_CONTENT_ID });

    const created = await api.raw('post', '/api/v1/acestream-channels/', {
      id: STUB_INFOHASH, name: 'E2E infohash result', id_type: 'infohash',
    });
    expect(created.status()).toBe(201);
    expect((await created.json()).id).toBe(STUB_INFOHASH_CONTENT_ID);
    // A previous run may have answered from the persistent conversion cache.
    expect((await api.getChannel(STUB_INFOHASH_CONTENT_ID))?.id).toBe(STUB_INFOHASH_CONTENT_ID);
    expect(await api.getChannel(STUB_INFOHASH)).toBeUndefined();

    const refused = await api.raw('post', '/api/v1/acestream-channels/', {
      id: UNKNOWN_INFOHASH, name: 'E2E unknown infohash', id_type: 'infohash',
    });
    expect(refused.status()).toBe(422);
    expect(stub.contentIdLookups()).toContain(UNKNOWN_INFOHASH);
    expect((await refused.json()).error.code).toBe('INFOHASH_UNRESOLVED');
    expect(await api.getChannel(UNKNOWN_INFOHASH)).toBeUndefined();
  } finally {
    await api.deleteChannel(STUB_INFOHASH_CONTENT_ID).catch(() => undefined);
    await api.putSetting('ace_engine_url', engineUrl);
    await stub.close();
  }
});
