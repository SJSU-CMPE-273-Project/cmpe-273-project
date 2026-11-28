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
  const [usage, setUsage] = useState({});
  const [account, setAccount] = useState('');
  const [notice, setNotice] = useState('');
  const [publicDemo, setPublicDemo] = useState(false);
  const [demoStatus, setDemoStatus] = useState({ active: false, cooldown_seconds: 0 });

  async function refresh() {
    try {
      const [m, d, t, c, p, u, config, demo] = await Promise.all([
        api('/api/metrics'), api('/api/dlq'), api('/api/deliveries'),
        api('/api/controls'), api('/api/agent/proposals'), api('/api/agent/usage'), api('/api/config'), api('/api/demo/status').catch(error => ({ active: false, cooldown_seconds: 0, error: error.message })),
      ]);
      setMetrics(m); setDlq(d.items); setDeliveries(t.items); setControls(c); setProposals(p.items); setUsage(u);
      setPublicDemo(config.publicDemo); setDemoStatus(demo);
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
    const proposal = proposals.find(p => p.id === id);
    if (proposal?.action === 'replay' && !confirm(`Approve replay of ${proposal.event_id}? The original event_id will be reused.`)) return;
    try { await api(`/api/agent/proposals/${id}/approve`, { method: 'POST' }); setNotice(proposal?.action === 'replay' ? 'Replay approved and queued for the delivery worker' : 'Proposal approved'); await refresh(); }
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

  async function runDemo() {
    try {
      const result = await api('/api/demo/run', { method: 'POST' });
      setNotice(`Safe ${result.scope} smoke test ${result.status}; up to ${result.max_events} synthetic events.`);
      await refresh();
    } catch (error) { setNotice(error.message); }
  }

  return <main>
    <header className="topbar">
      <div><span className="brandmark">RG</span><div><b>RelayGuard</b><span>Operations console</span></div></div>
      <div className="operator"><label>Operator <input defaultValue={operator} onChange={e => localStorage.setItem('relayguard_operator', e.target.value)} /></label><i>● Live</i></div>
    </header>

    {notice && <div className="notice"><span>{notice}</span><button onClick={() => setNotice('')}>Dismiss</button></div>}

    {publicDemo && <section className="panel demo-panel">
      <div className="panel-head"><div><p>Public demo</p><h2>Safe demonstration controls</h2></div><span className="demo-badge">No login · bounded actions</span></div>
      <div className="demo-actions">
        <button className="ghost" onClick={refresh}>Refresh live view</button>
        <button className="primary" disabled={demoStatus.active || demoStatus.cooldown_seconds > 0} onClick={runDemo}>
          {demoStatus.active ? 'Smoke test running…' : demoStatus.cooldown_seconds > 0 ? `Available in ${demoStatus.cooldown_seconds}s` : 'Run safe smoke test'}
        </button>
        <button className="ghost" onClick={() => window.open('/api/results', '_blank', 'noopener')}>View Oracle results</button>
      </div>
      {demoStatus.last_result && <p className="demo-result">Last smoke run: {demoStatus.last_error || JSON.stringify(demoStatus.last_result)}</p>}
      <p className="demo-note">The smoke test is fixed at 3 synthetic events, one per second. Target: {demoStatus.target || 'adapter'}{demoStatus.target === 'adapter' ? ' (does not exercise the worker)' : ' (Iggy ingest path; requires the worker)'}. Cooldown: 30 seconds.</p>
    </section>}

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
              <td>{item.reason}</td><td>
                <details className="attempt-details">
                  <summary>{item.attempts.length} attempt{item.attempts.length === 1 ? '' : 's'}</summary>
                  {item.attempts.map(attempt => <div className="attempt-entry" key={attempt.attempt}>
                    <b>#{attempt.attempt} · {attempt.channel} / {attempt.provider}</b>
                    <span>{attempt.succeeded ? 'Succeeded' : 'Failed'}{attempt.latency_ms != null ? ` · ${attempt.latency_ms} ms` : ''}</span>
                    {attempt.error && <span>{attempt.error}</span>}
                  </div>)}
                </details>
              </td>
              <td>{item.dead_lettered_at ? new Date(item.dead_lettered_at).toLocaleTimeString() : '—'}</td>
              {!publicDemo && <td className="actions">
                <button className="ghost" onClick={() => evaluate(item.event_id)}>Ask agent</button>
                <button className="primary" disabled={!!item.replayed_at || !!item.replay_status} onClick={() => replay(item.event_id)}>{item.replayed_at ? 'Replayed' : item.replay_status || 'Replay'}</button>
                {item.replay_status && <small>Replay {item.replay_status}: {item.replay_reason}</small>}
              </td>}
            </tr>)}</tbody>
          </table>
          {!dlq.length && <div className="empty">No dead-lettered messages</div>}
        </div>
      </article>

      {!publicDemo && <aside className="panel controls-panel">
        <div className="panel-head"><div><p>Guardrails</p><h2>Producer controls</h2></div></div>
        {controls.producers.map(p => <div className="producer" key={p.producer}>
          <div><b>{p.producer}</b><span>{p.max_per_minute} / minute</span></div>
          <button className={p.enabled ? 'switch on' : 'switch'} onClick={() => updateProducer(p.producer, { enabled: !p.enabled, max_per_minute: p.max_per_minute })} aria-label="Toggle producer"><i /></button>
        </div>)}
      </aside>}
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

    {!publicDemo && <section className="panel proposals-panel">
      <div className="panel-head"><div><p>Human in the loop</p><h2>Agent proposals</h2><span>Today (UTC): {Number(usage.input_tokens || 0) + Number(usage.output_tokens || 0)} tokens · {usage.priced === false ? "free tier, no charge" : `$${Number(usage.cost_usd || 0).toFixed(6)} estimated`} · budget {usage.budget_tokens || 0}/{usage.daily_budget || 0} (includes reservations)</span></div></div>
      {!proposals.length ? <div className="empty">No proposals</div> : proposals.map(p => <div className="proposal" key={p.id}>
        <div><span className={`action ${p.action}`}>{p.action}</span><code>{p.event_id?.slice(0, 8)}</code><small>Prompt {p.prompt_version} · {p.model} · {p.input_tokens} in / {p.output_tokens} out · {p.source}{p.usage_uncertain ? " · usage unknown; cost upper bound" : ""}</small></div>
        <p>{p.rationale}<small>Risk: {p.risk}</small>{p.replay_status && <small>Replay {p.replay_status}: {p.replay_reason}<br />{p.replay_workflow_id}</small>}</p>
        {p.approved == null ? <button className="primary" onClick={() => approve(p.id)}>Approve</button> : <span className="decision">{p.approved ? `Approved by ${p.approved_by}` : 'Rejected'}</span>}
      </div>)}
    </section>}
  </main>;
}

createRoot(document.getElementById('root')).render(<App />);
