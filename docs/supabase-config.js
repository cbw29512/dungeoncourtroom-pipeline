/*
  This file is intentionally committed as a plain browser config.
  Why this is safe:
  - Supabase's anon/publishable key is designed for frontend use when RLS is enabled.
  - Never put your service role key here.

  Fill these values from your Supabase project settings.
*/
window.DC_SUPABASE_CONFIG = {
  url: 'https://sxoaqxhiyovtqzxtjxqu.supabase.co',
  anonKey: 'sb_publishable_pHrrkDnZYr0iDVxjAwFOBA_6UApX6zM',
  seasonNumber: 1,
};
