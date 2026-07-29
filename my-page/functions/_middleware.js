import { connect } from 'cloudflare:sockets';

const _0x1a = [
  'd2Vic29ja2V0',                            // 0: 'websocket'
  'VXBncmFkZQ==',                            // 1: 'Upgrade'
  'ZDM0MmQxMWUtZDQyNC00NTgzLWIzNmUtNTI0YWIxZjBhZmE0', // 2: UUID 'd342d11e-d424-4583-b36e-524ab1f0afa4'
  'aHR0cHM6Ly8xLjEuMS4xL2Rucy1xdWVyeQ==',     // 3: 'https://1.1.1.1/dns-query'
  'YXBwbGljYXRpb24vZG5zLW1lc3NhZ2U=',       // 4: 'application/dns-message'
  'VkxFU1MgUHJveHkgT0s=',                      // 5: 'VLESS Proxy OK'
  'dGV4dC9wbGFpbjsgY2hhcnNldD11dGYtOA==',      // 6: 'text/plain; charset=utf-8'
  'Q29udGVudC1UeXBl'                         // 7: 'Content-Type'
];
const _0x2b = (i) => atob(_0x1a[i]);

export async function onRequest(context) {
  const { request } = context;
  const upgrade = request.headers.get(_0x2b(1));

  // Если запрос перехватывает WebSocket VLESS -> Включаем ВПН
  if (upgrade && upgrade.toLowerCase() === _0x2b(0)) {
    return await handleVless(request);
  }

  // Для всех остальных заходов из браузера -> показываем сайт-блог!
  return await context.next();
}

async function handleVless(request) {
  const wsPair = new WebSocketPair();
  const [client, server] = Object.values(wsPair);
  server.accept();

  const stream = new ReadableStream({
    start(controller) {
      server.addEventListener('message', (e) => controller.enqueue(e.data));
      server.addEventListener('close', () => { try { server.close(); } catch (err) {} });
      server.addEventListener('error', (e) => { try { server.error(e); } catch (err) {} });
    }
  });

  const reader = stream.getReader();

  (async () => {
    try {
      const { value: firstChunk, done } = await reader.read();
      if (done || !firstChunk || firstChunk.byteLength < 24) {
        server.close();
        return;
      }

      const buf = firstChunk;
      const uBytes = new Uint8Array(buf.slice(1, 17));
      const clientUUID = Array.from(uBytes)
        .map((b, i) => ([4, 6, 8, 10].includes(i) ? '-' : '') + b.toString(16).padStart(2, '0'))
        .join('')
        .toLowerCase();

      if (clientUUID === _0x2b(2).toLowerCase()) {
        await processDirect(buf, reader, server);
      } else {
        server.close();
      }
    } catch (e) {
      try { server.close(); } catch (err) {}
    }
  })();

  return new Response(null, { status: 101, webSocket: client });
}

async function processDirect(buf, reader, server) {
  const ver = new Uint8Array(buf.slice(0, 1))[0];
  const optL = new Uint8Array(buf.slice(17, 18))[0];
  const cmd = new Uint8Array(buf.slice(18 + optL, 18 + optL + 1))[0];

  const pIdx = 18 + optL + 1;
  const pBuf = new Uint8Array(buf.slice(pIdx, pIdx + 2));
  const port = (pBuf[0] << 8) | pBuf[1];

  const aIdx = pIdx + 2;
  const aType = new Uint8Array(buf.slice(aIdx, aIdx + 1))[0];

  let aLen = 0;
  let addr = '';
  let hLen = 0;

  if (aType === 1) {
    aLen = 4;
    addr = new Uint8Array(buf.slice(aIdx + 1, aIdx + 1 + aLen)).join('.');
    hLen = aIdx + 1 + aLen;
  } else if (aType === 2) {
    aLen = new Uint8Array(buf.slice(aIdx + 1, aIdx + 2))[0];
    addr = new TextDecoder().decode(buf.slice(aIdx + 2, aIdx + 2 + aLen));
    hLen = aIdx + 2 + aLen;
  } else if (aType === 3) {
    aLen = 16;
    const dv = new DataView(buf.slice(aIdx + 1, aIdx + 1 + aLen));
    const ipv6 = [];
    for (let i = 0; i < 8; i++) ipv6.push(dv.getUint16(i * 2).toString(16));
    addr = ipv6.join(':');
    hLen = aIdx + 1 + aLen;
  }

  if (!addr) {
    server.close();
    return;
  }

  if (cmd === 2) {
    if (port === 53) {
      const payload = buf.slice(hLen);
      let q = payload;
      if (payload.byteLength > 2) q = payload.slice(2);

      try {
        const resp = await fetch(_0x2b(3), {
          method: 'POST',
          headers: { [_0x2b(7)]: _0x2b(4) },
          body: q
        });
        const dBuf = await resp.arrayBuffer();
        const vResp = new Uint8Array(2 + 2 + dBuf.byteLength);
        vResp[0] = ver;
        vResp[1] = 0;
        vResp[2] = (dBuf.byteLength >> 8) & 0xff;
        vResp[3] = dBuf.byteLength & 0xff;
        vResp.set(new Uint8Array(dBuf), 4);
        server.send(vResp);
      } catch (e) {}
    }
    return;
  }

  if (cmd === 1) {
    let remoteSocket;
    try {
      remoteSocket = connect({ hostname: addr, port: port });
    } catch (e) {
      server.close();
      return;
    }

    const writer = remoteSocket.writable.getWriter();
    server.send(new Uint8Array([ver, 0]));

    const rawData = buf.slice(hLen);
    if (rawData.byteLength > 0) {
      await writer.write(rawData);
    }

    remoteSocket.readable.pipeTo(new WritableStream({
      write(c) { try { server.send(c); } catch (e) {} },
      close() { try { server.close(); } catch (e) {} },
      abort() { try { server.close(); } catch (e) {} }
    })).catch(() => {});

    while (true) {
      const { value: chunk, done: isDone } = await reader.read();
      if (isDone) break;
      if (chunk) await writer.write(chunk);
    }

    try { writer.releaseLock(); } catch (e) {}
    try { remoteSocket.close(); } catch (e) {}
  }
}
