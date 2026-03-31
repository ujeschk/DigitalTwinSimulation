import * as THREE from './libs/three.module.js';
import { OrbitControls } from './libs/OrbitControls.js';
import { IFCLoader } from './libs/IFCLoader.js';

const scene = new THREE.Scene();
scene.background = new THREE.Color(0x000000);

const camera = new THREE.PerspectiveCamera(75, window.innerWidth/window.innerHeight, 0.1, 1000);
const renderer = new THREE.WebGLRenderer();
renderer.setSize(window.innerWidth, window.innerHeight);
document.body.appendChild(renderer.domElement);

const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true;

const light = new THREE.DirectionalLight(0xffffff, 1);
light.position.set(0, 10, 0);
scene.add(light);

const ambient = new THREE.AmbientLight(0xffffff, 0.5);
scene.add(ambient);

const ifcLoader = new IFCLoader();
ifcLoader.ifcManager.setWasmPath("./libs/");

ifcLoader.load('model.ifc', (ifcModel) => {
  scene.add(ifcModel);
});

camera.position.set(8, 8, 8);

function animate() {
  requestAnimationFrame(animate);
  controls.update();
  renderer.render(scene, camera);
}
animate();

let sensorSphere = null;
let labelDiv = null;
let listenersAttached = false;

function clearPrevious() {
  if (sensorSphere) {
    scene.remove(sensorSphere);
    sensorSphere.geometry.dispose();
    sensorSphere.material.dispose();
    sensorSphere = null;
  }
  if (labelDiv) {
    document.body.removeChild(labelDiv);
    labelDiv = null;
  }
}

function parseTs(ts) {
  // "2025-08-10T22:29:54Z" veya "2025-08-10T22:29:54+00:00" uyumlu
  return new Date(ts);
}

/* ===== Mini panel helpers (only anomalies) ===== */
function relativeTime(iso) {
  const diff = (Date.now() - new Date(iso).getTime()) / 1000; // sec
  if (diff < 90) return `${Math.round(diff)}s ago`;
  if (diff < 5400) return `${Math.round(diff/60)}m ago`;
  return new Date(iso).toLocaleString();
}
function scoreClass(score) {
  // IsolationForest: skor ne kadar negatifse o kadar anormal
  if (score < -0.02) return 'bad';
  if (score < 0)     return 'mid';
  return 'good';
}
async function updateAnomalyPanel(limit = 10) {
  try {
    const res = await fetch('http://localhost:5000/api/anomalies?t=' + Date.now(), { cache: 'no-store' });
    const data = await res.json();

    // sadece anomalileri göster
    const rows = data
      .filter(r => r.is_anomaly === 1)
      .sort((a,b) => new Date(b.timestamp) - new Date(a.timestamp))
      .slice(0, limit);

    const list = document.getElementById('anomaly-list');
    if (!rows.length) {
      list.innerHTML = '<div class="row">No recent anomalies.</div>';
      return;
    }
    list.innerHTML = rows.map(r => `
      <div class="row">
        <span class="room">${r.room}</span>
        &nbsp;•&nbsp;
        <span class="score ${scoreClass(r.score)}">${Number(r.score).toFixed(3)}</span>
        <div class="time">${relativeTime(r.timestamp)}</div>
      </div>
    `).join('');
  } catch (e) {
    console.warn('panel fetch failed:', e);
    const list = document.getElementById('anomaly-list');
    if (list) list.textContent = 'Error loading anomalies.';
  }
}
/* =============================================== */

async function loadSensorData() {
  try {
    // 1) Telemetry: cache-break + no-store
    const res = await fetch('http://localhost:5000/api/telemetry-guid?t=' + Date.now(), { cache: 'no-store' });
    const data = await res.json();

    // --- EN ÖNEMLİ DÜZELTME: "en güncel" kaydı seç ---
    // (data[data.length-1] en eski olabiliyordu; şimdi max timestamp'i seçiyoruz)
    const latest = data.reduce((best, cur) => {
      if (!best) return cur;
      return new Date(cur.timestamp) > new Date(best.timestamp) ? cur : best;
    }, null);
    if (!latest) return;

    // 2) Anomalies (son 60 sn içinde mi?)
    let isRecentAnomaly = false;
    try {
      const ares = await fetch('http://localhost:5000/api/anomalies?t=' + Date.now(), { cache: 'no-store' });
      const anomalies = await ares.json();

      const tLatest = parseTs(latest.timestamp);
      // Aynı oda için son 60 sn içinde herhangi bir anomaly=1 var mı?
      isRecentAnomaly = anomalies.some(a =>
        a.room === latest.room &&
        a.is_anomaly === 1 &&
        Math.abs(new Date(a.timestamp) - tLatest) <= 60 * 1000
      );
    } catch (e) {
      console.warn('anomalies fetch failed:', e);
    }

    clearPrevious();

    const geometry = new THREE.SphereGeometry(0.1, 32, 32);
    // anomali son 60 sn içinde ise kırmızı, değilse yeşil
    const material = new THREE.MeshBasicMaterial({ color: isRecentAnomaly ? 0xff0000 : 0x00ff00 });
    sensorSphere = new THREE.Mesh(geometry, material);

    const adjustedZ = latest.z - 4.0; // mevcut z düzeltmen korunuyor
    sensorSphere.position.set(latest.x, latest.y, adjustedZ);
    scene.add(sensorSphere);

    // Label
    labelDiv = document.createElement('div');
    labelDiv.style.position = 'absolute';
    labelDiv.style.color = 'white';
    labelDiv.style.fontSize = '12px';
    labelDiv.style.textShadow = '1px 1px 2px black';
    labelDiv.style.pointerEvents = 'none';
    labelDiv.innerHTML = `
      <b>${latest.room_uri.split('#')[1]}</b><br>
      Temp: ${latest.temperature}°C<br>
      Humidity: ${latest.humidity}%<br>
      ${latest.timestamp}<br>
      ${isRecentAnomaly ? '<span style="color:red;font-weight:bold;">ANOMALY DETECTED</span>' : ''}
    `;
    document.body.appendChild(labelDiv);

    function updateLabel() {
      const vector = new THREE.Vector3(latest.x, latest.y, adjustedZ);
      vector.project(camera);
      const x = (vector.x * 0.5 + 0.5) * window.innerWidth;
      const y = -(vector.y * 0.5 - 0.5) * window.innerHeight;
      labelDiv.style.left = `${x}px`;
      labelDiv.style.top = `${y}px`;
    }

    updateLabel();
    if (!listenersAttached) {
      controls.addEventListener('change', updateLabel);
      window.addEventListener('resize', updateLabel);
      listenersAttached = true;
    }

    // paneli de tazele
    await updateAnomalyPanel(10);

  } catch (err) {
    console.error("Veri alınamadı:", err);
  }
}

// İlk veri yükle
loadSensorData();
// 5 saniyede bir güncelle
setInterval(loadSensorData, 5000);
// paneli de periyodik yenile
setInterval(updateAnomalyPanel, 5000);
