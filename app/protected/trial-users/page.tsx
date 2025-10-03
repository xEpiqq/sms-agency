import { redirect } from "next/navigation";
import { createClient } from "@/lib/supabase/server";
import { InfoIcon } from "lucide-react";

export default async function TrialUsersPage() {
  const supabase = await createClient();

  const { data, error } = await supabase.auth.getClaims();
  if (error || !data?.claims) {
    redirect("/auth/login");
  }

  return (
    <div className="flex-1 w-full flex flex-col gap-8">
      <div className="bg-accent/30 text-sm p-3 px-5 rounded-md text-foreground/90 flex gap-3 items-center border border-accent/40">
        <InfoIcon size={16} strokeWidth={2} />
        <span>Trial users</span>
      </div>

      <section className="rounded-2xl border border-foreground/10 bg-background/60 p-6 shadow-sm">
        <h1 className="text-2xl font-semibold tracking-tight">Trial users</h1>
        <p className="mt-2 text-sm opacity-70">
          Placeholder page. We can build this out next.
        </p>
      </section>
    </div>
  );
}
