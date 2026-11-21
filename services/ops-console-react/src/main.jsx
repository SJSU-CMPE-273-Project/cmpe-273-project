import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './styles.css';

const operator = localStorage.getItem('relayguard_operator') || 'operator';
const headers = {
  'Content-Type': 'application/json',
  'X-RelayGuard-Operator': operator,
};

async function api(path, options = {}) {
  const response = await fetch(path, { ...options, headers: { ...headers, ...options.headers } });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || 'Request failed');
  return body;
}

function Metric({ label, value, tone }) {
  return <article className={`metric ${tone || ''}`}><span>{label}</span><strong>{value}</strong></article>;
}

function App() {
  const [metrics, setMetrics] = useState({});
  const [dlq, setDlq] = useState([]);
  const [deliveries, setDeliveries] = useState([]);
  const [controls, setControls] = useState({ producers: [], guardrails: [] });
  const [proposals, setProposals] = useState([]);
  const [account, setAccount] = useState('');
  const [notice, setNotice] = useState('');

  async function refresh() {
    try {
      const [m, d, t, c, p] = await Promise.all([
        api('/api/metrics'), api('/api/dlq'), api('/api/deliveries'),
        api('/api/controls'), api('/api/agent/proposals'),
      ]);
      setMetrics(m); setDlq(d.items); setDeliveries(t.items); setControls(c); setProposals(p.items);
    } catch (error) { setNotice(error.message); }
  }

  useEffect(() => { refresh(); const timer = setInterval(refresh, 5000); return () => clearInterval(timer); }, []);

  async function replay(eventId) {
    if (!confirm(`Replay ${eventId}? The original event_id will be reused.`)) return;
    try { await api(`/api/dlq/${eventId}/replay`, { method: 'POST' }); setNotice('Replay approved and recorded'); await refresh(); }
    catch (error) { setNotice(error.message); }
  }

  async function evaluate(eventId) {
    try { await api(`/api/agent/evaluate/${eventId}`, { method: 'POST' }); setNotice('Agent proposal created'); await refresh(); }
    catch (error) { setNotice(error.message); }
  }

  async function approve(id) {
    try { await api(`/api/agent/proposals/${id}/approve`, { method: 'POST' }); setNotice('Proposal approved'); await refresh(); }
    catch (error) { setNotice(error.message); }
  }

  async function updateProducer(producer, patch) {
    try { await api(`/api/controls/producers/${encodeURIComponent(producer)}`, { method: 'PUT', body: JSON.stringify(patch) }); await refresh(); }
    catch (error) { setNotice(error.message); }
  }

  async function searchTimeline(event) {
    event.preventDefault();
    try { const data = await api(`/api/deliveries${account ? `?account=${encodeURIComponent(account)}` : ''}`); setDeliveries(data.items); }
    catch (error) { setNotice(error.message); }
  }

  return <main>
    <header className="topbar">
      <div><span className="brandmark">RG</span><div><b>RelayGuard</b><span>Operations console</span></div></div>
      <div className="operator"><label>Operator <input defaultValue={operator} onChange={e => localStorage.setItem('relayguard_operator', e.target.value)} /></label><i>● Live</i></div>
    </header>

    {notice && <div className="notice"><span>{notice}</span><button onClick={() => setNotice('')}>Dismiss</button></div>}

    <section className="metrics">
      <Metric label="Delivered" value={metrics.delivered ?? '—'} tone="green" />
      <Metric label="Retrying" value={metrics.retrying ?? '—'} tone="amber" />
      <Metric label="Dead letters" value={metrics.dlq ?? '—'} tone="red" />
      <Metric label="Ingest rejects" value={metrics.ingest_rejections ?? '—'} />
      <Metric label="p99 latency" value={metrics.p99_latency_ms != null ? `${metrics.p99_latency_ms} ms` : '—'} />
    </section>

    <section className="grid">
      <article className="panel dlq-panel">
        <div className="panel-head"><div><p>Recovery</p><h2>Dead-letter queue</h2></div><span className="count">{dlq.length}</span></div>
        <div className="table-wrap">
          <table><thead><tr><th>Event</th><th>Reason</th><th>Attempts</th><th>Age</th><th></th></tr></thead>
            <tbody>{dlq.map(item => <tr key={item.event_id}>
              <td><code>{item.event_id.slice(0, 8)}</code><small>{item.account_id} · seq {item.sequence}</small></td>
              <td>{item.reason}</td><td>{item.attempts}</td>
              <td>{item.dead_lettered_at ? new Date(item.dead_lettered_at).toLocaleTimeString() : '—'}</td>
              <td className="actions">
                <button className="ghost" onClick={() => evaluate(item.event_id)}>Ask agent</button>
                <button className="primary" disabled={!!item.replayed_at} onClick={() => replay(item.event_id)}>{item.replayed_at ? 'Replayed' : 'Replay'}</button>
              </td>
            </tr>)}</tbody>
          </table>
          {!dlq.length && <div className="empty">No dead-lettered messages</div>}
        </div>
      </article>

      <aside className="panel controls-panel">
        <div className="panel-head"><div><p>Guardrails</p><h2>Producer controls</h2></div></div>
        {controls.producers.map(p => <div className="producer" key={p.producer}>
          <div><b>{p.producer}</b><span>{p.max_per_minute} / minute</span></div>
          <button className={p.enabled ? 'switch on' : 'switch'} onClick={() => updateProducer(p.producer, { enabled: !p.enabled, max_per_minute: p.max_per_minute })} aria-label="Toggle producer"><i /></button>
        </div>)}
      </aside>
    </section>

    <section className="panel timeline-panel">
      <div className="panel-head"><div><p>Trace</p><h2>Delivery timeline</h2></div>
        <form onSubmit={searchTimeline}><input aria-label="Account ID" placeholder="Filter account ID" value={account} onChange={e => setAccount(e.target.value)} /><button>Filter</button></form>
      </div>
      <div className="table-wrap"><table><thead><tr><th>Sequence</th><th>Event</th><th>State</th><th>Channel</th><th>Attempts</th><th>Updated</th></tr></thead>
        <tbody>{deliveries.map(d => <tr key={d.event_id}><td>{d.sequence}</td><td><code>{d.event_id.slice(0, 8)}</code><small>{d.account_id}</small></td>
          <td><span className={`state ${d.state}`}>{d.state.replace('_', ' ')}</span></td><td>{d.channel || '—'}<small>{d.provider || ''}</small></td><td>{d.attempts}</td><td>{new Date(d.updated_at).toLocaleTimeString()}</td></tr>)}</tbody>
      </table></div>
    </section>

    <section className="panel proposals-panel">
      <div className="panel-head"><div><p>Human in the loop</p><h2>Agent proposals</h2></div></div>
      {!proposals.length ? <div className="empty">No proposals</div> : proposals.map(p => <div className="proposal" key={p.id}>
        <div><span className={`action ${p.action}`}>{p.action}</span><code>{p.event_id?.slice(0, 8)}</code><small>Prompt {p.prompt_version}</small></div>
        <p>{p.rationale}</p>
        {p.approved == null ? <button className="primary" onClick={() => approve(p.id)}>Approve</button> : <span className="decision">{p.approved ? `Approved by ${p.approved_by}` : 'Rejected'}</span>}
      </div>)}
    </section>
  </main>;
}

createRoot(document.getElementById('root')).render(<App />);
