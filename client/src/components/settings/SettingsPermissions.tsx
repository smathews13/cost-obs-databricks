import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";

interface UserPermissions {
  admins: string[];
  consumers: string[];
  table_location?: string | null;
}

export function SettingsPermissions() {
  const queryClient = useQueryClient();
  const [newAdmin, setNewAdmin] = useState("");
  const [newConsumer, setNewConsumer] = useState("");

  const { data: permissions, isLoading } = useQuery<UserPermissions>({
    queryKey: ["user-permissions"],
    queryFn: async () => {
      const res = await fetch("/api/settings/user-permissions");
      if (!res.ok) throw new Error("Failed to fetch");
      return res.json();
    },
  });

  const saveMutation = useMutation({
    mutationFn: async (data: UserPermissions) => {
      const res = await fetch("/api/settings/user-permissions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? "Failed to save");
      }
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["user-permissions"] });
      queryClient.refetchQueries({ queryKey: ["user"] });
    },
  });

  const addAdmin = () => {
    const email = newAdmin.trim();
    if (!email) return;
    saveMutation.mutate({
      admins: [...(permissions?.admins ?? []), email],
      consumers: (permissions?.consumers ?? []).filter((e) => e !== email),
    });
    setNewAdmin("");
  };

  const removeAdmin = (email: string) => {
    saveMutation.mutate({
      admins: (permissions?.admins ?? []).filter((e) => e !== email),
      consumers: permissions?.consumers ?? [],
    });
  };

  const addConsumer = () => {
    const email = newConsumer.trim();
    if (!email) return;
    saveMutation.mutate({
      admins: (permissions?.admins ?? []).filter((e) => e !== email),
      consumers: [...(permissions?.consumers ?? []), email],
    });
    setNewConsumer("");
  };

  const removeConsumer = (email: string) => {
    saveMutation.mutate({
      admins: permissions?.admins ?? [],
      consumers: (permissions?.consumers ?? []).filter((e) => e !== email),
    });
  };

  if (isLoading) {
    return <div className="py-8 text-center text-sm text-gray-500">Loading permissions...</div>;
  }

  return (
    <div className="space-y-6">
      {saveMutation.isError && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          <strong>Save failed:</strong> {saveMutation.error instanceof Error ? saveMutation.error.message : "Unknown error"}. Check that the app service principal has INSERT/DELETE access to the permissions table.
        </div>
      )}
      <div className="rounded-lg border border-blue-100 bg-blue-50 p-4 text-sm text-blue-700">
        <strong>Default access:</strong> Any user not explicitly listed is treated as a <strong>Consumer</strong>. Add users to <em>Admins</em> to grant settings access.
      </div>

      {permissions?.table_location && (
        <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-xs text-gray-600">
          <span className="font-medium">Permissions table: </span>
          <code className="rounded bg-gray-100 px-1.5 py-0.5 font-mono text-gray-800">{permissions.table_location}</code>
          <span className="ml-2 text-gray-400">— stored in Unity Catalog, persists across deploys</span>
        </div>
      )}

      {/* Admins */}
      <div>
        <h4 className="mb-1 text-sm font-semibold text-gray-800">Admins</h4>
        <p className="mb-3 text-xs text-gray-500">Admins can view all data and change app settings.</p>
        <div className="mb-3 space-y-2">
          {(permissions?.admins ?? []).length === 0 ? (
            <p className="text-xs text-gray-400 italic">No explicit admins listed — all unlisted users are admins by default.</p>
          ) : (
            (permissions?.admins ?? []).map((email) => (
              <div key={email} className="flex items-center justify-between rounded-lg border border-gray-200 bg-white px-3 py-2">
                <div className="flex items-center gap-2">
                  <span className="rounded bg-gray-100 px-1.5 py-0.5 text-xs font-medium text-gray-600">Admin</span>
                  <span className="text-sm text-gray-800">{email}</span>
                </div>
                <button
                  onClick={() => removeAdmin(email)}
                  className="text-xs text-red-500 hover:text-red-700"
                >
                  Remove
                </button>
              </div>
            ))
          )}
        </div>
        <div className="flex gap-2">
          <input
            type="email"
            placeholder="user@example.com"
            value={newAdmin}
            onChange={(e) => setNewAdmin(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && addAdmin()}
            className="flex-1 rounded-md border border-gray-300 px-3 py-1.5 text-sm focus:border-[#FF3621] focus:outline-none"
          />
          <button
            onClick={addAdmin}
            disabled={!newAdmin.trim() || saveMutation.isPending}
            className="btn-brand rounded-md px-4 py-1.5 text-sm font-medium text-white disabled:opacity-50"
          >
            Add Admin
          </button>
        </div>
      </div>

      {/* Consumers */}
      <div>
        <h4 className="mb-1 text-sm font-semibold text-gray-800">Consumers</h4>
        <p className="mb-3 text-xs text-gray-500">Consumers can view dashboards but cannot change app settings.</p>
        <div className="mb-3 space-y-2">
          {(permissions?.consumers ?? []).length === 0 ? (
            <p className="text-xs text-gray-400 italic">No consumers listed.</p>
          ) : (
            (permissions?.consumers ?? []).map((email) => (
              <div key={email} className="flex items-center justify-between rounded-lg border border-gray-200 bg-white px-3 py-2">
                <div className="flex items-center gap-2">
                  <span className="rounded bg-gray-100 px-1.5 py-0.5 text-xs font-medium text-gray-500">Consumer</span>
                  <span className="text-sm text-gray-800">{email}</span>
                </div>
                <button
                  onClick={() => removeConsumer(email)}
                  className="text-xs text-red-500 hover:text-red-700"
                >
                  Remove
                </button>
              </div>
            ))
          )}
        </div>
        <div className="flex gap-2">
          <input
            type="email"
            placeholder="user@example.com"
            value={newConsumer}
            onChange={(e) => setNewConsumer(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && addConsumer()}
            className="flex-1 rounded-md border border-gray-300 px-3 py-1.5 text-sm focus:border-[#FF3621] focus:outline-none"
          />
          <button
            onClick={addConsumer}
            disabled={!newConsumer.trim() || saveMutation.isPending}
            className="btn-brand rounded-md px-4 py-1.5 text-sm font-medium text-white disabled:opacity-50"
          >
            Add Consumer
          </button>
        </div>
      </div>
    </div>
  );
}
