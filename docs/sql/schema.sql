-- Dungeon Courtroom schema for GitHub Pages + Supabase.
-- Run this in the Supabase SQL editor.

create extension if not exists pgcrypto;

create table if not exists public.approved_monsters (
  name text primary key,
  source text not null default 'SRD 5.1',
  is_srd_safe boolean not null default true
);

insert into public.approved_monsters (name, source, is_srd_safe)
values
  ('Acolyte', 'SRD 5.1', true),
  ('Bugbear', 'SRD 5.1', true),
  ('Cultist', 'SRD 5.1', true),
  ('Ghoul', 'SRD 5.1', true),
  ('Goblin', 'SRD 5.1', true),
  ('Hobgoblin', 'SRD 5.1', true),
  ('Imp', 'SRD 5.1', true),
  ('Kobold', 'SRD 5.1', true),
  ('Lizardfolk', 'SRD 5.1', true),
  ('Ogre', 'SRD 5.1', true),
  ('Orc', 'SRD 5.1', true),
  ('Skeleton', 'SRD 5.1', true),
  ('Spectator', 'SRD 5.1', true),
  ('Sprite', 'SRD 5.1', true)
on conflict (name) do nothing;

create table if not exists public.episodes (
  id uuid primary key default gen_random_uuid(),
  slug text unique not null,
  season_number integer not null,
  episode_number integer not null,
  title text not null,
  summary text not null,
  youtube_url text not null,
  reddit_url text,
  question_title text not null,
  question_body text not null,
  monster_law_name text not null,
  monster_cool_name text not null,
  judge_name text not null default 'The DM Judge',
  bailiff_name text not null default 'The Bailiff',
  thumbnail_url text,
  is_featured boolean not null default false,
  status text not null default 'published' check (status in ('draft', 'published', 'archived')),
  published_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (season_number, episode_number)
);

alter table public.episodes add column if not exists is_featured boolean not null default false;

create table if not exists public.votes (
  id uuid primary key default gen_random_uuid(),
  episode_id uuid not null references public.episodes(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  vote_value text not null check (vote_value in ('law', 'cool', 'tie')),
  created_at timestamptz not null default now(),
  unique (episode_id, user_id)
);

create table if not exists public.submissions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users(id) on delete set null,
  title text not null,
  body text not null,
  reddit_url text,
  submitter_name text,
  status text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
  created_at timestamptz not null default now()
);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_episodes_updated_at on public.episodes;
create trigger trg_episodes_updated_at
before update on public.episodes
for each row
execute function public.set_updated_at();

create or replace function public.validate_episode_monsters()
returns trigger
language plpgsql
as $$
begin
  if not exists (
    select 1 from public.approved_monsters where name = new.monster_law_name and is_srd_safe = true
  ) then
    raise exception 'monster_law_name must be an approved SRD-safe monster: %', new.monster_law_name;
  end if;

  if not exists (
    select 1 from public.approved_monsters where name = new.monster_cool_name and is_srd_safe = true
  ) then
    raise exception 'monster_cool_name must be an approved SRD-safe monster: %', new.monster_cool_name;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_validate_episode_monsters on public.episodes;
create trigger trg_validate_episode_monsters
before insert or update on public.episodes
for each row
execute function public.validate_episode_monsters();

create or replace function public.get_vote_totals(p_episode_id uuid)
returns table (
  law_count bigint,
  cool_count bigint,
  tie_count bigint,
  total_count bigint
)
language sql
security definer
set search_path = public
as $$
  select
    count(*) filter (where vote_value = 'law') as law_count,
    count(*) filter (where vote_value = 'cool') as cool_count,
    count(*) filter (where vote_value = 'tie') as tie_count,
    count(*) as total_count
  from public.votes
  where episode_id = p_episode_id;
$$;

revoke all on function public.get_vote_totals(uuid) from public;
grant execute on function public.get_vote_totals(uuid) to anon, authenticated;

create index if not exists episodes_season_status_episode_idx
on public.episodes (season_number, status, is_featured desc, published_at desc, episode_number desc);

create index if not exists votes_episode_vote_idx
on public.votes (episode_id, vote_value);

create index if not exists submissions_status_created_idx
on public.submissions (status, created_at desc);

alter table public.episodes enable row level security;
alter table public.votes enable row level security;
alter table public.submissions enable row level security;

drop policy if exists "episodes_public_read" on public.episodes;
create policy "episodes_public_read"
on public.episodes
for select
to anon, authenticated
using (status = 'published');

drop policy if exists "votes_owner_read" on public.votes;
create policy "votes_owner_read"
on public.votes
for select
to authenticated
using (auth.uid() = user_id);

drop policy if exists "votes_authenticated_insert" on public.votes;
create policy "votes_authenticated_insert"
on public.votes
for insert
to authenticated
with check (auth.uid() = user_id);

drop policy if exists "submissions_authenticated_insert" on public.submissions;
create policy "submissions_authenticated_insert"
on public.submissions
for insert
to authenticated
with check (auth.uid() = user_id);

insert into public.episodes (
  slug,
  season_number,
  episode_number,
  title,
  summary,
  youtube_url,
  reddit_url,
  question_title,
  question_body,
  monster_law_name,
  monster_cool_name,
  thumbnail_url,
  is_featured,
  status
)
select
  'pilot-hearing',
  1,
  1,
  'Dungeon Courtroom Pilot | Rule of Cool vs Rule of Law',
  'The court hears its first dispute as two SRD-safe monsters argue opposite sides of a wild tabletop ruling.',
  'https://www.youtube.com/channel/UCMVN3DYPMADGKw9e9CZ13Rg',
  'https://www.reddit.com/r/DungeonCourtroom/',
  'Should the ruling follow the written law or the awesome moment?',
  'The pilot puts a dramatic table ruling on trial and asks whether written rules or cinematic play should win.',
  'Goblin',
  'Ogre',
  'assets/pilot-poster.jpg',
  true,
  'published'
where not exists (
  select 1 from public.episodes where slug = 'pilot-hearing'
);
