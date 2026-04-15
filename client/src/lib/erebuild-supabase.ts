import { createClient } from "@supabase/supabase-js";

// Erebuild ERP Supabase — read-only for office_locations setting
const EREBUILD_URL = "https://fhtwtpkviidcpqpohnak.supabase.co";
const EREBUILD_ANON_KEY =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZodHd0cGt2aWlkY3BxcG9obmFrIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDg4NjY4MSwiZXhwIjoyMDkwNDYyNjgxfQ.iZAaYOw0DBuRiftiA8RozEQIh46pRWmmgfgrPUlDJIg";

export const erebuildSupabase = createClient(EREBUILD_URL, EREBUILD_ANON_KEY);
