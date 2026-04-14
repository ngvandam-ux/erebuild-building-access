import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = "https://jvecvoqzyxsicsrrpvyu.supabase.co";
const SUPABASE_ANON_KEY =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp2ZWN2b3F6eXhzaWNzcnJwdnl1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU4OTg3NzQsImV4cCI6MjA5MTQ3NDc3NH0.in9zxmrRkvgwj3jdUDNmB8jzPcAoybX_6BtGT25T8Qg";

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
