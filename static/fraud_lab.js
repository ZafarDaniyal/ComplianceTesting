const state = {
  summary: null,
  sampleById: new Map(),
  activeSampleId: '',
};

const refs = {
  metricStrip: document.getElementById('metricStrip'),
  sampleGrid: document.getElementById('sampleGrid'),
  scoreForm: document.getElementById('scoreForm'),
  shuffleBtn: document.getElementById('shuffleBtn'),
  category: document.getElementById('category'),
  gender: document.getElementById('gender'),
  amount: document.getElementById('amount'),
  age: document.getElementById('age'),
  cityPop: document.getElementById('cityPop'),
  transactionAt: document.getElementById('transactionAt'),
  zip: document.getElementById('zip'),
  distanceMiles: document.getElementById('distanceMiles'),
  formHint: document.getElementById('formHint'),
  verdictLabel: document.getElementById('verdictLabel'),
  riskBand: document.getElementById('riskBand'),
  dial: document.getElementById('dial'),
  probabilityValue: document.getElementById('probabilityValue'),
  thresholdValue: document.getElementById('thresholdValue'),
  decisionSummary: document.getElementById('decisionSummary'),
  signalGrid: document.getElementById('signalGrid'),
  mathFormula: document.getElementById('mathFormula'),
  mathStats: document.getElementById('mathStats'),
  positiveReasons: document.getElementById('positiveReasons'),
  negativeReasons: document.getElementById('negativeReasons'),
  contributionRows: document.getElementById('contributionRows'),
  performanceGrid: document.getElementById('performanceGrid'),
  importanceList: document.getElementById('importanceList'),
};

const genderLabel = {
  F: 'Profile F',
  M: 'Profile M',
};

const riskCopy = {
  routine: { badge: 'Looks routine', title: 'Looks routine' },
  guarded: { badge: 'Worth a look', title: 'Worth a look' },
  elevated: { badge: 'Needs attention', title: 'Needs attention' },
  critical: { badge: 'Flag for review', title: 'Flag for review' },
};

function money(value) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(Number(value || 0));
}

function pct(value, digits = 2) {
  return `${Number(value || 0).toFixed(digits)}%`;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
    body: options.body,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || 'Request failed');
  }
  return payload;
}

function renderMetricStrip(summary) {
  const metrics = [
    ['ROC-AUC', summary.metrics.roc_auc.toFixed(4)],
    ['PR-AUC', summary.metrics.pr_auc.toFixed(4)],
    ['Precision', summary.metrics.precision.toFixed(4)],
    ['Recall', summary.metrics.recall.toFixed(4)],
  ];

  refs.metricStrip.innerHTML = metrics
    .map(([label, value]) => `
      <article class="metric-chip">
        <span>${label}</span>
        <strong>${value}</strong>
      </article>
    `)
    .join('');
}

function populateSelect(select, options) {
  select.innerHTML = options
    .map((option) => {
      const label = select === refs.gender ? (genderLabel[option.value] || option.label) : option.label;
      return `<option value="${option.value}">${label}</option>`;
    })
    .join('');
}

function renderSamples(samples) {
  state.sampleById.clear();
  samples.forEach((sample) => state.sampleById.set(sample.id, sample));

  refs.sampleGrid.innerHTML = samples
    .map((sample) => {
      const friendly = riskCopy[sample.risk_band] || { badge: sample.risk_band };
      return `
        <button class="sample-card" type="button" data-sample-id="${sample.id}">
          <div class="sample-meta">
            <span>${pct(sample.probability_pct)}</span>
            <span class="tone-pill ${sample.tone}">${friendly.badge}</span>
          </div>
          <h3>${sample.name}</h3>
          <div class="sample-meta">
            <span>${sample.payload.distance_miles.toFixed(1)} miles from home</span>
            <span>${sample.payload.category.replaceAll('_', ' ')}</span>
          </div>
        </button>
      `;
    })
    .join('');

  refs.sampleGrid.querySelectorAll('[data-sample-id]').forEach((button) => {
    button.addEventListener('click', () => {
      const sampleId = button.dataset.sampleId;
      loadSample(sampleId);
      scoreCurrentTransaction();
    });
  });
}

function setActiveSample(sampleId) {
  state.activeSampleId = sampleId;
  refs.sampleGrid.querySelectorAll('[data-sample-id]').forEach((button) => {
    button.classList.toggle('active', button.dataset.sampleId === sampleId);
  });
}

function loadSample(sampleId) {
  const sample = state.sampleById.get(sampleId);
  if (!sample) {
    return;
  }

  const payload = sample.payload;
  refs.amount.value = payload.amount;
  refs.category.value = payload.category;
  refs.gender.value = payload.gender;
  refs.age.value = payload.age;
  refs.cityPop.value = payload.city_pop;
  refs.transactionAt.value = payload.transaction_at;
  refs.zip.value = payload.zip;
  refs.distanceMiles.value = payload.distance_miles.toFixed(1);
  refs.formHint.textContent = `${sample.name} loaded. This preset currently scores ${pct(sample.probability_pct)} and keeps the technical details hidden.`;
  setActiveSample(sampleId);
}

function formPayload() {
  return {
    amount: Number(refs.amount.value),
    category: refs.category.value,
    gender: refs.gender.value,
    age: Number(refs.age.value),
    city_pop: Number(refs.cityPop.value),
    transaction_at: refs.transactionAt.value,
    zip: Number(refs.zip.value),
    distance_miles: Number(refs.distanceMiles.value),
  };
}

function renderSignals(result) {
  const items = [
    ['Amount', money(result.normalized.amount)],
    ['Category', result.normalized.category_label],
    ['Time signal', `${result.engineered_features.hour}:00 / ${result.engineered_features.day_name}`],
    ['Night flag', result.engineered_features.is_night ? 'Yes' : 'No'],
    ['Weekend flag', result.engineered_features.is_weekend ? 'Yes' : 'No'],
    ['Distance from home', `${result.engineered_features.distance_miles.toFixed(1)} miles`],
    ['Customer profile', genderLabel[result.normalized.gender] || result.normalized.gender],
  ];

  refs.signalGrid.innerHTML = items
    .map(([label, value]) => `
      <article class="signal-card">
        <span>${label}</span>
        <strong>${value}</strong>
      </article>
    `)
    .join('');
}

function renderMath(result) {
  refs.mathFormula.textContent = `Behind the scenes: z = ${result.math.bias_log_odds.toFixed(4)} + feature effects = ${result.math.raw_log_odds.toFixed(4)}, then p = 1 / (1 + e^-z).`;

  const stats = [
    ['Bias term', result.math.bias_log_odds.toFixed(4)],
    ['Raw log-odds', result.math.raw_log_odds.toFixed(4)],
    ['Sigmoid output', result.probability_from_margin.toFixed(6)],
  ];

  refs.mathStats.innerHTML = stats
    .map(([label, value]) => `
      <article class="performance-card">
        <span>${label}</span>
        <strong>${value}</strong>
      </article>
    `)
    .join('');

  refs.positiveReasons.innerHTML = renderReasonCards(result.math.top_positive, 'positive');
  refs.negativeReasons.innerHTML = renderReasonCards(result.math.top_negative, 'negative');

  const maxAbs = Math.max(...result.math.all_features.map((item) => item.abs_contribution), 0.0001);
  refs.contributionRows.innerHTML = result.math.all_features
    .map((item) => {
      const width = `${(item.abs_contribution / maxAbs) * 100}%`;
      const signClass = item.contribution >= 0 ? 'positive' : 'negative';
      const signed = item.contribution >= 0 ? `+${item.contribution.toFixed(4)}` : item.contribution.toFixed(4);
      return `
        <article class="contribution-row">
          <div>
            <strong>${item.label}</strong>
            <span>${item.value} · ${item.impact.replace('fraud risk', 'review risk')}</span>
            <div class="bar-track">
              <div class="bar-fill ${signClass}" style="width:${width};"></div>
            </div>
          </div>
          <div class="mono ${signClass}">${signed}</div>
        </article>
      `;
    })
    .join('');
}

function renderReasonCards(items, polarity) {
  if (!items.length) {
    return '<p class="empty-state">No dominant signals in this direction.</p>';
  }

  return items
    .map((item) => {
      const signed = item.contribution >= 0 ? `+${item.contribution.toFixed(4)}` : item.contribution.toFixed(4);
      return `
        <article class="reason-card">
          <strong>${item.label}</strong>
          <span>${item.value}</span>
          <div class="delta ${polarity}">${signed} log-odds</div>
        </article>
      `;
    })
    .join('');
}

function renderPerformance(summary) {
  const metrics = [
    ['Review line', pct(summary.metrics.threshold * 100, 1)],
    ['Overall balance', summary.metrics.f1.toFixed(4)],
    ['False alarms', String(summary.metrics.false_positives)],
    ['Missed fraud cases', String(summary.metrics.false_negatives)],
  ];

  refs.performanceGrid.innerHTML = metrics
    .map(([label, value]) => `
      <article class="performance-card">
        <span>${label}</span>
        <strong>${value}</strong>
      </article>
    `)
    .join('');

  const maxPct = Math.max(...summary.feature_importance.map((item) => item.gain_pct), 0.0001);
  refs.importanceList.innerHTML = summary.feature_importance
    .map((item) => `
      <article class="importance-row">
        <div>
          <strong>${item.label}</strong>
          <span>${item.gain_pct.toFixed(2)}% of gain</span>
          <div class="bar-track">
            <div class="bar-fill" style="width:${(item.gain_pct / maxPct) * 100}%;"></div>
          </div>
        </div>
        <div class="mono">${item.gain_pct.toFixed(2)}%</div>
      </article>
    `)
    .join('');
}

function renderDecision(result) {
  const probability = result.probability_pct;
  const friendly = riskCopy[result.risk_band] || { badge: result.risk_band, title: result.verdict };
  refs.probabilityValue.textContent = pct(probability);
  refs.thresholdValue.textContent = pct(result.threshold_pct, 1);
  refs.verdictLabel.textContent = friendly.title;
  refs.riskBand.textContent = friendly.badge;
  refs.riskBand.className = `risk-band ${result.risk_band}`;
  refs.dial.style.setProperty('--dial-fill', `${Math.max(0, Math.min(100, probability)) / 100}turn`);

  if (result.verdict === 'Fraud') {
    refs.decisionSummary.textContent = `${pct(probability)} clears the review line, so this transaction would be escalated for human review.`;
  } else if (result.risk_band === 'elevated' || result.risk_band === 'guarded') {
    refs.decisionSummary.textContent = `${pct(probability)} is noticeable, but it stays below the review line.`;
  } else {
    refs.decisionSummary.textContent = `${pct(probability)} stays comfortably below the review line.`;
  }
}

async function scoreCurrentTransaction() {
  try {
    refs.formHint.textContent = 'Checking how the system would react...';
    const result = await api('/api/fraud/score', {
      method: 'POST',
      body: JSON.stringify(formPayload()),
    });
    renderDecision(result);
    renderSignals(result);
    renderMath(result);
    refs.formHint.textContent = 'Plain-language view first. The math is still available lower on the page.';
  } catch (error) {
    refs.formHint.textContent = error.message;
  }
}

function randomSampleId() {
  const samples = Array.from(state.sampleById.keys());
  if (!samples.length) {
    return '';
  }
  const candidates = samples.filter((id) => id !== state.activeSampleId);
  const source = candidates.length ? candidates : samples;
  return source[Math.floor(Math.random() * source.length)];
}

async function init() {
  try {
    const summary = await api('/api/fraud/model');
    state.summary = summary;
    renderMetricStrip(summary);
    populateSelect(refs.category, summary.category_options);
    populateSelect(refs.gender, summary.gender_options);
    renderSamples(summary.sample_transactions);
    renderPerformance(summary);

    const kickoff = [...summary.sample_transactions].sort((left, right) => right.probability_pct - left.probability_pct)[0];
    if (kickoff) {
      loadSample(kickoff.id);
      await scoreCurrentTransaction();
    }
  } catch (error) {
    refs.formHint.textContent = error.message;
    refs.metricStrip.innerHTML = `<article class="metric-chip"><span>Status</span><strong>Unavailable</strong></article>`;
  }
}

refs.scoreForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  await scoreCurrentTransaction();
});

refs.shuffleBtn.addEventListener('click', async () => {
  const sampleId = randomSampleId();
  if (!sampleId) {
    return;
  }
  loadSample(sampleId);
  await scoreCurrentTransaction();
});

init();
