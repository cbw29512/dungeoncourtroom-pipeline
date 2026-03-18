# Dungeon Courtroom — GitHub Pages + Supabase MVP (docs-ready)

## Objective

This package is the corrected GitHub Pages + Supabase site for Dungeon Courtroom.
It is designed to live inside your existing repo under:

```text
/docs
```

## What was fixed

- newest or explicitly featured episode now becomes the homepage feature
- vote buttons now check the current anonymous browser user correctly
- duplicate vote click handlers were removed
- public raw vote reads were removed from the browser path
- vote totals now come from a secure aggregate RPC: `get_vote_totals(...)`
- SRD-safe monster enforcement was added through `approved_monsters` + a validation trigger
- schema now includes useful indexes and an `updated_at` trigger
- repo layout instructions now match GitHub Pages from `main /docs`

## Folder Layout

```text
/docs
  index.html
  season-1.html
  styles.css
  app.js
  supabase-config.js
  .nojekyll
  /assets
  /sql
```

## Supabase setup

1. Create a Supabase project.
2. In **Authentication → Providers**, enable **Anonymous Sign-Ins**.
3. Open the SQL editor and run `sql/schema.sql`.
4. In **Project Settings → API**, copy:
   - Project URL
   - anon / publishable key
5. Edit `supabase-config.js` and paste those values.

### Security rule

Use only the **anon / publishable** key in `supabase-config.js`.
Never expose the **service role** key in a browser file.

## GitHub Pages deploy

Because this is meant to live inside your existing repo, the simplest GitHub Pages setup is:

1. Put these files in your repo under `/docs`
2. Commit and push
3. In GitHub repo **Settings → Pages**:
   - Source: **Deploy from a branch**
   - Branch: `main`
   - Folder: `/docs`
4. Save

GitHub Pages will look for `index.html` at the top level of `/docs`.

## Current MVP limits

- Anonymous auth is stronger than plain localStorage, but clearing browser data can create a fresh anonymous identity.
- There is no admin dashboard yet.
- Turnstile / CAPTCHA is still recommended before going big.

## Recommended next upgrade

Build a tiny admin page that:
- approves submissions
- creates new episodes
- toggles the featured episode flag
