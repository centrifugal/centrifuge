// Shared map_clients presence viewer.
// Each page sets window.GRID_CONFIG before loading this script:
//   {
//     channel:  'clients:massive_100k' | 'clients:massive_1m',
//     poolSize: 102400 | 1048576,
//     cols:     320    | 1024,
//     rows:     320    | 1024,
//     cellFill: 2      | 1,         // pixels of filled square per cell
//     cellGap:  1      | 0,         // pixels of background between cells
//     useImageData: false | true,   // batch-fill the bulk sync via ImageData
//   }

(function () {
  const cfg = window.GRID_CONFIG;
  if (!cfg) {
    document.body.textContent = 'Missing window.GRID_CONFIG';
    return;
  }

  const { channel, poolSize, cols, rows, cellFill, cellGap, useImageData } = cfg;
  const cellStride = cellFill + cellGap;
  const canvasSize = cols * cellStride;

  // Lay out the page to match the canvas size — gaps render unevenly when
  // the canvas is fractionally scaled by CSS width:100%.
  document.documentElement.style.setProperty('--canvas-size', canvasSize + 'px');
  document.documentElement.style.setProperty('--container-width', (canvasSize + 26) + 'px');

  const canvas = document.getElementById('grid');
  canvas.width = canvasSize;
  canvas.height = canvasSize;

  const ctx = canvas.getContext('2d', { alpha: false });
  const OCCUPIED_COLOR = '#3a7bd5';
  const EMPTY_COLOR = '#0a1020';
  const OCCUPIED_R = 0x3a, OCCUPIED_G = 0x7b, OCCUPIED_B = 0xd5;

  ctx.fillStyle = EMPTY_COLOR;
  ctx.fillRect(0, 0, canvasSize, canvasSize);

  // Single source of truth: 1 = occupied, 0 = empty.
  const occupied = new Uint8Array(poolSize);
  let occupiedCount = 0;
  // One in-flight flash per cell — a fast leave→join cancels the leave.
  const flashByCell = new Map();
  let drainHandle = 0;

  // ---- DOM refs ----
  const statusEl = document.getElementById('status');
  const statusTextEl = document.getElementById('statusText');
  const totalUsersEl = document.getElementById('totalUsers');
  const totalCountEl = document.getElementById('totalCount');
  const joinRateEl = document.getElementById('joinRate');
  const leaveRateEl = document.getElementById('leaveRate');
  const totalUpdatesEl = document.getElementById('totalUpdates');
  const syncBar = document.getElementById('syncBar');
  totalUsersEl.textContent = poolSize.toLocaleString();

  // ---- Cell painting ----
  function cellForKey(key) {
    if (!key || key.length < 3 || key.charCodeAt(0) !== 99 || key.charCodeAt(1) !== 95) return -1;
    const n = +key.slice(2);
    return Number.isInteger(n) && n >= 0 && n < poolSize ? n : -1;
  }

  function drawCell(idx, fill) {
    const x = (idx % cols) * cellStride;
    const y = ((idx / cols) | 0) * cellStride;
    ctx.fillStyle = fill;
    ctx.fillRect(x, y, cellFill, cellFill);
  }

  function clearCell(idx) {
    drawCell(idx, EMPTY_COLOR);
  }

  function addEntry(key, silent) {
    const idx = cellForKey(key);
    if (idx < 0 || occupied[idx]) return;
    occupied[idx] = 1;
    occupiedCount++;
    drawCell(idx, OCCUPIED_COLOR);
    if (silent) flashByCell.delete(idx);
    else flashByCell.set(idx, { until: performance.now() + 600, color: '#80e27e' });
  }

  function removeEntry(key) {
    const idx = cellForKey(key);
    if (idx < 0 || !occupied[idx]) return;
    occupied[idx] = 0;
    occupiedCount--;
    flashByCell.set(idx, { until: performance.now() + 400, color: '#ff8a80' });
  }

  function tick(now) {
    for (const [idx, f] of flashByCell) {
      if (now < f.until) {
        drawCell(idx, f.color);
      } else {
        drawCell(idx, occupied[idx] ? OCCUPIED_COLOR : EMPTY_COLOR);
        flashByCell.delete(idx);
      }
    }
    requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);

  // ---- Rates ----
  let joinsThisSec = 0;
  let leavesThisSec = 0;
  let updatesTotal = 0;
  setInterval(() => {
    joinRateEl.textContent = joinsThisSec;
    leaveRateEl.textContent = leavesThisSec;
    joinsThisSec = 0;
    leavesThisSec = 0;
  }, 1000);

  // ---- Centrifuge ----
  let centrifuge = null;
  let sub = null;

  function setStatus(text, connected) {
    statusTextEl.textContent = text;
    statusEl.classList.toggle('connected', !!connected);
  }

  function resetGrid() {
    if (drainHandle) { cancelAnimationFrame(drainHandle); drainHandle = 0; }
    occupied.fill(0);
    occupiedCount = 0;
    flashByCell.clear();
    ctx.fillStyle = EMPTY_COLOR;
    ctx.fillRect(0, 0, canvasSize, canvasSize);
    totalCountEl.textContent = '0';
    syncBar.style.width = '0%';
  }

  function resetAll() {
    resetGrid();
    totalUpdatesEl.textContent = '0';
    updatesTotal = 0;
  }

  // Bulk fill via ImageData — calls fillRect once per cell otherwise; for
  // 1M cells that blocks for seconds. Writes only the cellFill x cellFill
  // block per cell; the gap pixels stay at the EMPTY_COLOR set by the
  // reset above.
  function bulkFillImageData(renderQueue, total, onProgress, onDone) {
    const imgData = ctx.getImageData(0, 0, canvasSize, canvasSize);
    const data = imgData.data;
    let i = 0;
    const chunkSize = 100000;
    const drainChunk = () => {
      const end = Math.min(i + chunkSize, total);
      for (; i < end; i++) {
        const idx = renderQueue[i];
        if (!occupied[idx]) continue;
        const cellX = (idx % cols) * cellStride;
        const cellY = ((idx / cols) | 0) * cellStride;
        for (let dy = 0; dy < cellFill; dy++) {
          const row = (cellY + dy) * canvasSize;
          for (let dx = 0; dx < cellFill; dx++) {
            const off = (row + cellX + dx) * 4;
            data[off]     = OCCUPIED_R;
            data[off + 1] = OCCUPIED_G;
            data[off + 2] = OCCUPIED_B;
            data[off + 3] = 255;
          }
        }
      }
      ctx.putImageData(imgData, 0, 0);
      onProgress(i, total);
      if (i < total) drainHandle = requestAnimationFrame(drainChunk);
      else { drainHandle = 0; onDone(); }
    };
    drainHandle = requestAnimationFrame(drainChunk);
  }

  // Bulk fill via fillRect — simpler, fine up to ~200k cells.
  function bulkFillRect(renderQueue, total, onProgress, onDone) {
    let i = 0;
    const chunkSize = 5000;
    const drainChunk = () => {
      const end = Math.min(i + chunkSize, total);
      for (; i < end; i++) {
        const idx = renderQueue[i];
        if (!occupied[idx]) continue;
        drawCell(idx, OCCUPIED_COLOR);
      }
      onProgress(i, total);
      if (i < total) drainHandle = requestAnimationFrame(drainChunk);
      else { drainHandle = 0; onDone(); }
    };
    drainHandle = requestAnimationFrame(drainChunk);
  }

  const PAGE_SIZE_KEY = 'mapPresenceMassive.pageSize.' + channel;
  const pageSizeInput = document.getElementById('pageSize');
  const defaultPageSize = useImageData ? '10000' : '1000';
  pageSizeInput.value = localStorage.getItem(PAGE_SIZE_KEY) || defaultPageSize;

  function connect() {
    const pageSize = parseInt(pageSizeInput.value, 10) || +defaultPageSize;
    localStorage.setItem(PAGE_SIZE_KEY, String(pageSize));

    if (centrifuge) { centrifuge.disconnect(); centrifuge = null; }
    resetAll();

    centrifuge = new Centrifuge('ws://' + window.location.host + '/connection/websocket', {
      name: 'viewer'
    });
    centrifuge.on('connecting', () => setStatus('Connecting…', false));
    centrifuge.on('connected',  () => setStatus('Connected', true));
    centrifuge.on('disconnected', () => setStatus('Disconnected', false));

    sub = centrifuge.newMapClientsSubscription(channel, { pageSize });

    sub.on('sync', (syncCtx) => {
      resetGrid();
      const entries = syncCtx.entries || [];
      const renderQueue = [];
      for (let i = 0; i < entries.length; i++) {
        const pub = entries[i];
        if (!pub.key || pub.removed) continue;
        const idx = cellForKey(pub.key);
        if (idx < 0 || occupied[idx]) continue;
        occupied[idx] = 1;
        occupiedCount++;
        renderQueue.push(idx);
      }
      totalCountEl.textContent = occupiedCount.toLocaleString();
      const total = renderQueue.length;
      if (total === 0) { syncBar.style.width = '0%'; return; }
      const onProgress = (i, t) => { syncBar.style.width = ((i / t) * 100).toFixed(1) + '%'; };
      const onDone = () => {
        syncBar.style.width = '100%';
        setTimeout(() => { syncBar.style.width = '0%'; }, 400);
      };
      if (useImageData) bulkFillImageData(renderQueue, total, onProgress, onDone);
      else              bulkFillRect(renderQueue, total, onProgress, onDone);
    });

    sub.on('update', (ctx) => {
      if (!ctx.key) return;
      if (ctx.removed) { removeEntry(ctx.key); leavesThisSec++; }
      else             { addEntry(ctx.key);    joinsThisSec++; }
      updatesTotal++;
      totalUpdatesEl.textContent = updatesTotal.toLocaleString();
      totalCountEl.textContent = occupiedCount.toLocaleString();
    });

    sub.subscribe();
    centrifuge.connect();
  }

  document.getElementById('apply').addEventListener('click', connect);
  connect();
})();
