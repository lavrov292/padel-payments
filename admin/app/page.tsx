"use client";

import React from "react";  
import { useEffect, useState } from "react";
import { supabase } from "@/lib/supabase";

type Tournament = {
  tournament_id: number;
  title: string;
  starts_at: string;
  price_rub: number;
  entries_total: number;
  entries_paid: number;
  entries_pending: number;
};

const apiBase = process.env.NEXT_PUBLIC_API_BASE;
if (!apiBase) {
  throw new Error("NEXT_PUBLIC_API_BASE environment variable is not set");
}

export default function AdminPage() {
  const [openTournamentId, setOpenTournamentId] = useState<number | null>(null);
  const [entries, setEntries] = useState<any[]>([]);
  const [data, setData] = useState<Tournament[]>([]);
  const [loading, setLoading] = useState(true);
  const [errorText, setErrorText] = useState<string | null>(null);
  const [copiedEntryId, setCopiedEntryId] = useState<number | null>(null);

  useEffect(() => {
    async function load() {
      setLoading(true);
      setErrorText(null);

      const { data, error } = await supabase
        .from("admin_tournaments_view")
        .select("*");

      if (error) {
        console.error(error);
        setErrorText(`${error.message}`);
      } else {
        setData((data as Tournament[]) || []);
      }

      setLoading(false);
    }

    load();
  }, []);

  if (loading) return <div className="p-6 bg-gray-900 min-h-screen text-white">Загрузка...</div>;

  if (errorText) {
    return (
      <div className="p-6">
        <div className="text-red-600 font-semibold mb-2">Ошибка Supabase</div>
        <pre className="text-xs whitespace-pre-wrap">{errorText}</pre>
        <div className="mt-4 text-sm">
          Проверь <code>admin/.env.local</code> и наличие view{" "}
          <code>admin_tournaments_view</code>.
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 bg-gray-900 min-h-screen">
      <h1 className="text-2xl font-bold mb-6 text-white">Турниры</h1>
      <table className="border border-gray-500 w-full text-sm">
        <thead>
          <tr className="bg-black text-white">
            <th className="border border-gray-500 px-2 py-1 text-left">Турнир</th>
            <th className="border border-gray-500 px-2 py-1 text-left">Дата</th>
            <th className="border border-gray-500 px-2 py-1 text-left">Цена</th>
            <th className="border border-gray-500 px-2 py-1">Всего</th>
            <th className="border border-gray-500 px-2 py-1">Оплачено</th>
            <th className="border border-gray-500 px-2 py-1">Не оплачено</th>
          </tr>
        </thead>

        <tbody>
  {data.map((t) => (
    <React.Fragment key={t.tournament_id}>
      <tr
        key={t.tournament_id}
        className="bg-gray-800 text-white cursor-pointer hover:bg-gray-700"
        onClick={async () => {
          // toggle
          if (openTournamentId === t.tournament_id) {
            setOpenTournamentId(null);
            setEntries([]);
            return;
          }

          const { data: rows, error } = await supabase
            .from("admin_entries_view")
            .select("*")
            .eq("tournament_id", t.tournament_id);

          if (error) {
            console.error(error);
            return;
          }

          setEntries(rows || []);
          setOpenTournamentId(t.tournament_id);
        }}
      >
        <td className="border border-gray-500 px-2 py-1">{t.title}</td>
        <td className="border border-gray-500 px-2 py-1">
          {new Date(t.starts_at).toLocaleString()}
        </td>
        <td className="border border-gray-500 px-2 py-1">{t.price_rub} ₽</td>
        <td className="border border-gray-500 px-2 py-1 text-center">{t.entries_total}</td>
        <td className="border border-gray-500 px-2 py-1 text-center">{t.entries_paid}</td>
        <td className="border border-gray-500 px-2 py-1 text-center">{t.entries_pending}</td>
      </tr>

      {openTournamentId === t.tournament_id && (
        <tr key={`details-${t.tournament_id}`}>
          <td colSpan={6} className="border border-gray-500 px-2 py-3 bg-gray-800 text-white">
            <div className="font-semibold mb-2">Участники</div>

            <table className="w-full text-sm">
              <thead>
                <tr className="bg-black text-white text-left">
                  <th className="border border-gray-500 py-1 px-2">Игрок</th>
                  <th className="border border-gray-500 py-1 px-2">Статус</th>
                  <th className="border border-gray-500 py-1 px-2">Уведомление</th>
                  <th className="border border-gray-500 py-1 px-2">Ссылка</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((e: any) => (
                  <tr key={e.entry_id} className="border-t border-gray-500 bg-gray-800 text-white">
                    <td className="py-1 px-2 border border-gray-500">{e.full_name}</td>
                    <td className="py-1 px-2 border border-gray-500">
                      {e.payment_status}
                      {e.manual_paid && (
                        <span className="ml-2 px-1.5 py-0.5 text-xs bg-yellow-500 text-white rounded">
                          manual
                        </span>
                      )}
                    </td>
                    <td className="py-1 px-2 border border-gray-500">
                      {e.telegram_id ? (
                        e.telegram_notified ? (
                          <span className="text-green-400">✅ Отправлено</span>
                        ) : (
                          <span className="text-yellow-400">⏳ Не отправлено</span>
                        )
                      ) : (
                        <span className="text-gray-400">— Нет Telegram</span>
                      )}
                    </td>
                    <td className="py-1 px-2 border border-gray-500">
                      <div className="flex items-center gap-2 flex-wrap">
                        {e.payment_status === "paid" ? (
                          <span className="text-green-400 text-xs">Оплачено</span>
                        ) : (
                          <>
                            {e.payment_url && (
                              <>
                                <button
                                  className="px-3 py-1.5 text-xs font-medium bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
                                  onClick={async () => {
                                    try {
                                      await navigator.clipboard.writeText(e.payment_url);
                                      setCopiedEntryId(e.entry_id);
                                      setTimeout(() => setCopiedEntryId(null), 2000);
                                    } catch (err) {
                                      alert("Не удалось скопировать ссылку");
                                    }
                                  }}
                                >
                                  Скопировать ссылку
                                </button>
                                {copiedEntryId === e.entry_id && (
                                  <span className="text-xs text-green-400">Скопировано!</span>
                                )}
                              </>
                            )}
                            {e.payment_status === "pending" && (
                              <button
                                className="px-3 py-1.5 text-xs font-medium bg-red-600 text-white rounded hover:bg-red-700 transition-colors"
                                onClick={async () => {
                                  if (!confirm("Отметить оплату вручную?")) return;
                                  
                                  const res = await fetch(
                                    `${apiBase}/admin/entries/${e.entry_id}/mark-manual-paid`,
                                    {
                                      method: "POST",
                                      headers: { "Content-Type": "application/json" },
                                      body: JSON.stringify({ note: "manual" }),
                                    }
                                  );
                                  
                                  const json = await res.json();
                                  if (json.ok) {
                                    // Перезагружаем список entries
                                    const { data: rows } = await supabase
                                      .from("admin_entries_view")
                                      .select("*")
                                      .eq("tournament_id", t.tournament_id);
                                    setEntries(rows || []);
                                    
                                    // Перезагружаем список турниров
                                    const { data: fresh } = await supabase
                                      .from("admin_tournaments_view")
                                      .select("*");
                                    setData(fresh || []);
                                  } else {
                                    alert(`Ошибка: ${json.error || "неизвестная ошибка"}`);
                                  }
                                }}
                              >
                                Отметить оплату (вручную)
                              </button>
                            )}
                            {!e.payment_url && e.payment_status !== "pending" && (
                              <span className="text-gray-400 text-xs">—</span>
                            )}
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}

                {entries.length === 0 && (
                  <tr>
                    <td colSpan={4} className="py-2 px-2 border border-gray-500 text-gray-400">
                      Нет записей
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </td>
        </tr>
      )}
    </React.Fragment>
  ))}
</tbody>
      </table>
    </div>
  );
}