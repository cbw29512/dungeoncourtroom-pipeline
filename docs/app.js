import { createClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm';

/*
  Dungeon Courtroom frontend
  - Static-site friendly for GitHub Pages
  - Durable state stored in Supabase
  - Anonymous auth gives each browser a stable user id without a full signup flow
*/

const state = {
  supabase: null,
  config: null,
  currentUser: null,
  featuredEpisode: null,
  featuredUserVote: null,
  episodes: [],
};

function getConfig() {
  try {
    return window.DC_SUPABASE_CONFIG || null;
  } catch (error) {
    console.error('Config read failed:', error);
    return null;
  }
}

function configLooksReady(config) {
  return Boolean(
    config &&
      typeof config.url === 'string' &&
      config.url.startsWith('https://') &&
      !config.url.includes('YOUR-PROJECT') &&
      typeof config.anonKey === 'string' &&
      !config.anonKey.includes('YOUR_SUPABASE_ANON_KEY'),
  );
}

function showSetupWarning(message) {
  document.querySelectorAll('[data-setup-warning]').forEach((node) => {
    node.textContent = message;
    node.style.display = 'block';
  });
}

function hideSetupWarning() {
  document.querySelectorAll('[data-setup-warning]').forEach((node) => {
    node.style.display = 'none';
  });
}

function setFeaturedStatus(message) {
  const node = document.querySelector('[data-featured-status]');
  if (node) node.textContent = message;
}

function wireMobileMenu() {
  const menuToggle = document.querySelector('.menu-toggle');
  const navLinks = document.querySelector('.nav-links');

  if (!menuToggle || !navLinks) return;

  menuToggle.addEventListener('click', () => {
    const isOpen = navLinks.classList.toggle('is-open');
    menuToggle.setAttribute('aria-expanded', String(isOpen));
  });
}

async function initSupabase() {
  const config = getConfig();
  state.config = config;

  if (!configLooksReady(config)) {
    showSetupWarning('Supabase is not configured yet. Edit supabase-config.js and add your project URL + anon key.');
    return false;
  }

  try {
    state.supabase = createClient(config.url, config.anonKey, {
      auth: {
        persistSession: true,
        autoRefreshToken: true,
      },
    });

    const sessionResult = await state.supabase.auth.getSession();
    const existingSession = sessionResult?.data?.session;

    if (!existingSession) {
      const signInResult = await state.supabase.auth.signInAnonymously();
      if (signInResult.error) throw signInResult.error;
    }

    const userResult = await state.supabase.auth.getUser();
    state.currentUser = userResult?.data?.user || null;

    if (!state.currentUser?.id) {
      throw new Error('Anonymous sign-in did not return a user id.');
    }

    hideSetupWarning();
    return true;
  } catch (error) {
    console.error('Supabase init failed:', error);
    showSetupWarning('Supabase could not initialize. Double-check your keys and enable Anonymous Sign-Ins in Supabase Auth.');
    return false;
  }
}

async function loadEpisodes() {
  if (!state.supabase || !state.config) return;

  try {
    const { data, error } = await state.supabase
      .from('episodes')
      .select('*')
      .eq('season_number', state.config.seasonNumber || 1)
      .eq('status', 'published')
      .order('is_featured', { ascending: false })
      .order('published_at', { ascending: false })
      .order('episode_number', { ascending: false });

    if (error) throw error;

    state.episodes = Array.isArray(data) ? data : [];
    state.featuredEpisode = state.episodes.find((episode) => episode.is_featured) || state.episodes[0] || null;
  } catch (error) {
    console.error('Episode load failed:', error);
    renderEpisodeLoadFailure();
  }
}

function renderEpisodeLoadFailure() {
  const homeEpisodes = document.querySelector('[data-home-episodes]');
  const seasonEpisodes = document.querySelector('[data-season-episodes]');

  if (homeEpisodes) {
    homeEpisodes.innerHTML = '<article class="episode-card panel"><h3>Episodes could not be loaded.</h3><p>Check your Supabase tables and RLS policies.</p></article>';
  }

  if (seasonEpisodes) {
    seasonEpisodes.innerHTML = '<article class="episode-card panel"><h3>Episodes could not be loaded.</h3><p>Check your Supabase tables and RLS policies.</p></article>';
  }

  setFeaturedStatus('The court record could not be read.');
}

function updateFeaturedPanel() {
  const episode = state.featuredEpisode;
  if (!episode) {
    setFeaturedStatus('No published episode found yet.');
    return;
  }

  const seasonNode = document.querySelector('[data-featured-season]');
  const matchupNode = document.querySelector('[data-featured-matchup]');
  const summaryNode = document.querySelector('[data-featured-summary]');

  if (seasonNode) {
    seasonNode.textContent = `Season ${episode.season_number} · Episode ${episode.episode_number}`;
  }

  if (matchupNode) {
    matchupNode.textContent = `${episode.monster_law_name} vs ${episode.monster_cool_name}`;
  }

  if (summaryNode) {
    summaryNode.textContent = episode.summary || episode.question_body || 'No summary provided yet.';
  }
}

function renderHomeEpisodes() {
  const container = document.querySelector('[data-home-episodes]');
  if (!container) return;

  if (!state.episodes.length) {
    container.innerHTML = '<article class="episode-card panel"><h3>No episodes published yet.</h3><p>Seed the episodes table to populate the docket.</p></article>';
    return;
  }

  const cards = state.episodes.slice(0, 3).map((episode) => `
    <article class="episode-card panel">
      <span class="episode-badge">Episode ${escapeHtml(String(episode.episode_number))}</span>
      <h3>${escapeHtml(episode.title)}</h3>
      <p>${escapeHtml(episode.monster_law_name)} vs ${escapeHtml(episode.monster_cool_name)}</p>
      <div class="episode-actions">
        <a class="button button-secondary small" href="season-1.html#episode-${escapeAttribute(String(episode.id))}">View</a>
        <a class="button button-primary small" href="${escapeAttribute(episode.youtube_url || '#')}" target="_blank" rel="noreferrer">Watch</a>
      </div>
    </article>
  `).join('');

  container.innerHTML = cards;
}

function renderSeasonEpisodes() {
  const container = document.querySelector('[data-season-episodes]');
  if (!container) return;

  if (!state.episodes.length) {
    container.innerHTML = '<article class="episode-card panel"><h3>No episodes published yet.</h3><p>Seed the episodes table to populate the archive.</p></article>';
    return;
  }

  const cards = state.episodes.map((episode) => `
    <article class="episode-card panel" id="episode-${escapeAttribute(String(episode.id))}">
      <span class="episode-badge">Episode ${escapeHtml(String(episode.episode_number))}</span>
      <h3>${escapeHtml(episode.title)}</h3>
      <p>${escapeHtml(episode.monster_law_name)} vs ${escapeHtml(episode.monster_cool_name)}</p>
      <p class="archive-summary">${escapeHtml(episode.summary || episode.question_body || 'No summary yet.')}</p>
      <div class="episode-actions">
        <a class="button button-primary small" href="${escapeAttribute(episode.youtube_url || '#')}" target="_blank" rel="noreferrer">Watch</a>
        ${episode.reddit_url ? `<a class="button button-secondary small" href="${escapeAttribute(episode.reddit_url)}" target="_blank" rel="noreferrer">Discuss</a>` : ''}
      </div>
    </article>
  `).join('');

  container.innerHTML = cards;
}

function updateVoteButtons(scope, hasVoted, selectedVote) {
  const buttons = scope.querySelectorAll('[data-vote]');

  buttons.forEach((button) => {
    const voteType = button.getAttribute('data-vote');
    button.disabled = hasVoted;
    button.classList.toggle('is-selected', Boolean(selectedVote && voteType === selectedVote));
  });
}

function wireVoteButtons(scope, episodeId) {
  const buttons = scope.querySelectorAll('[data-vote]');

  buttons.forEach((button) => {
    if (button.dataset.wired === 'true') return;

    button.dataset.wired = 'true';
    button.addEventListener('click', async () => {
      await castVote(episodeId, button.getAttribute('data-vote'));
    });
  });
}

async function loadFeaturedVoteData() {
  const scope = document.querySelector('[data-featured-votes]');
  const episode = state.featuredEpisode;

  if (!scope || !episode || !state.supabase || !state.currentUser?.id) return;

  try {
    const { data: totalsRow, error: totalsError } = await state.supabase
      .rpc('get_vote_totals', { p_episode_id: episode.id })
      .single();

    if (totalsError) throw totalsError;

    const totals = {
      law: Number(totalsRow?.law_count || 0),
      cool: Number(totalsRow?.cool_count || 0),
      tie: Number(totalsRow?.tie_count || 0),
    };

    ['law', 'cool', 'tie'].forEach((type) => {
      const node = scope.querySelector(`[data-count="${type}"]`);
      if (node) node.textContent = String(totals[type]);
    });

    const { data: existingVote, error: voteError } = await state.supabase
      .from('votes')
      .select('vote_value')
      .eq('episode_id', episode.id)
      .eq('user_id', state.currentUser.id)
      .maybeSingle();

    if (voteError) throw voteError;

    state.featuredUserVote = existingVote?.vote_value || null;
    wireVoteButtons(scope, episode.id);
    updateVoteButtons(scope, Boolean(state.featuredUserVote), state.featuredUserVote);

    if (state.featuredUserVote) {
      setFeaturedStatus(`Your verdict is already on record: ${labelForVote(state.featuredUserVote)}.`);
    } else {
      setFeaturedStatus('Choose your verdict. Anonymous sign-in keeps one vote per browser user unless browser data is cleared.');
    }
  } catch (error) {
    console.error('Vote data load failed:', error);
    setFeaturedStatus('Vote totals could not be loaded.');
  }
}

async function castVote(episodeId, voteValue) {
  if (!state.supabase || !episodeId || !voteValue) return;
  if (state.featuredUserVote) {
    setFeaturedStatus(`Your verdict is already on record: ${labelForVote(state.featuredUserVote)}.`);
    return;
  }

  try {
    setFeaturedStatus('Submitting your verdict…');

    const { error } = await state.supabase
      .from('votes')
      .insert({
        episode_id: episodeId,
        user_id: state.currentUser.id,
        vote_value: voteValue,
      });

    if (error) {
      if (String(error.message || '').toLowerCase().includes('duplicate') || String(error.code || '') === '23505') {
        state.featuredUserVote = voteValue;
        setFeaturedStatus('This browser user already voted on the featured case.');
        await loadFeaturedVoteData();
        return;
      }
      throw error;
    }

    state.featuredUserVote = voteValue;
    setFeaturedStatus(`Verdict recorded: ${labelForVote(voteValue)}.`);
    await loadFeaturedVoteData();
  } catch (error) {
    console.error('Vote submit failed:', error);
    setFeaturedStatus('Your vote could not be recorded.');
  }
}

function labelForVote(type) {
  if (type === 'law') return 'Rule of Law';
  if (type === 'cool') return 'Rule of Cool';
  return 'Tie / Hung Jury';
}

function isValidUrl(value) {
  if (!value) return true;

  try {
    const parsed = new URL(value);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

function wireSubmissionForm() {
  const form = document.querySelector('[data-submit-case-form]');
  const status = document.querySelector('[data-submit-status]');

  if (!form) return;

  form.addEventListener('submit', async (event) => {
    event.preventDefault();

    if (!state.supabase || !state.currentUser?.id) {
      if (status) status.textContent = 'Supabase is not ready yet. Configure supabase-config.js first.';
      return;
    }

    const formData = new FormData(form);
    const payload = {
      title: String(formData.get('case_title') || '').trim(),
      body: String(formData.get('case_details') || '').trim(),
      reddit_url: String(formData.get('reddit_link') || '').trim() || null,
      submitter_name: String(formData.get('submitter_name') || '').trim() || null,
    };

    if (!payload.title || !payload.body) {
      if (status) status.textContent = 'Case title and case details are required.';
      return;
    }

    if (payload.title.length > 160) {
      if (status) status.textContent = 'Case title must stay under 160 characters.';
      return;
    }

    if (payload.body.length > 5000) {
      if (status) status.textContent = 'Case details must stay under 5000 characters.';
      return;
    }

    if (!isValidUrl(payload.reddit_url)) {
      if (status) status.textContent = 'Reddit link must be a valid http or https URL.';
      return;
    }

    try {
      if (status) status.textContent = 'Submitting to the court clerk…';

      const { error } = await state.supabase
        .from('submissions')
        .insert({
          title: payload.title,
          body: payload.body,
          reddit_url: payload.reddit_url,
          submitter_name: payload.submitter_name,
          user_id: state.currentUser.id,
        });

      if (error) throw error;

      form.reset();
      if (status) status.textContent = 'Case submitted. The court clerk has it in the pending queue.';
    } catch (error) {
      console.error('Submission failed:', error);
      if (status) status.textContent = 'Submission failed. Check RLS policies and table setup.';
    }
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function escapeAttribute(value) {
  return escapeHtml(value);
}

async function boot() {
  wireMobileMenu();
  wireSubmissionForm();

  const ready = await initSupabase();
  if (!ready) return;

  await loadEpisodes();
  updateFeaturedPanel();
  renderHomeEpisodes();
  renderSeasonEpisodes();
  await loadFeaturedVoteData();
}

boot().catch((error) => {
  console.error('Boot failed:', error);
});
