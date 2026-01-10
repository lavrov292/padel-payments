"use client";

import React from "react";  
import { useEffect, useState } from "react";
import { supabase } from "@/lib/supabase";

type Tournament = {
  tournament_id: number;
  title: string;
  location: string | null;
  starts_at: string;
  price_rub: number;
  tournament_type: string;
  active: boolean;
  archived_at: string | null;
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
  const [copiedEntryId, setCopiedEntryId] = useState<string | number | null>(null);
  const [showPast, setShowPast] = useState(false);
  const [showArchived, setShowArchived] = useState(false);

  useEffect(() => {
    async function load() {
      setLoading(true);
      setErrorText(null);

      // Load tournaments - show archived if checkbox is checked
      let query = supabase
        .from("tournaments")
        .select("id, title, location, starts_at, price_rub, tournament_type, active, archived_at, first_seen_in_source, last_seen_in_source");
      
      // Only filter by archived_at if showArchived is false
      if (!showArchived) {
        query = query.is("archived_at", null);  // Only show non-archived tournaments
      }
      
      const { data: tournamentsData, error: tournamentsError } = await query
        .order("starts_at", { ascending: true });
      
      if (tournamentsError) {
        setErrorText(tournamentsError.message);
        setLoading(false);
        return;
      }
      
      // Load entries separately to count paid/pending
      const { data: entriesData, error: entriesError } = await supabase
        .from("entries")
        .select("tournament_id, payment_status");
      
      if (entriesError) {
        console.error("Error loading entries:", entriesError);
      }
      
      // Process data
      const entriesByTournament = new Map<number, any[]>();
      if (entriesData) {
        entriesData.forEach((entry: any) => {
          const tid = entry.tournament_id;
          if (!entriesByTournament.has(tid)) {
            entriesByTournament.set(tid, []);
          }
          entriesByTournament.get(tid)!.push(entry);
        });
      }
      
      const processed = (tournamentsData || []).map((t: any) => {
        const entries = entriesByTournament.get(t.id) || [];
        const paid = entries.filter((e: any) => e.payment_status === 'paid').length;
        const pending = entries.filter((e: any) => e.payment_status === 'pending').length;
        
        return {
          tournament_id: t.id,
          title: t.title,
          location: t.location || null,
          starts_at: t.starts_at,
          price_rub: t.price_rub,
          tournament_type: t.tournament_type || 'personal',
          active: t.active !== false,
          archived_at: t.archived_at,
          entries_total: entries.length,
          entries_paid: paid,
          entries_pending: pending
        } as Tournament;
      });
      
      const error = entriesError ? entriesError.message : null;

      if (error) {
        console.error(error);
        setErrorText(error);
      } else {
        setData(processed);
      }

      setLoading(false);
    }

    load();
  }, [showArchived]);

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

  // Filter tournaments
  const now = new Date();
  const filteredData = data.filter((t) => {
    // Filter by archived_at (not active)
    if (!showArchived && t.archived_at !== null) {
      return false;
    }
    
    // Filter by past
    if (!showPast) {
      const tournamentDate = new Date(t.starts_at);
      if (tournamentDate < now) {
        return false;
      }
    }
    
    return true;
  });

  // Format date in MSK
  const formatMSK = (dateStr: string) => {
    return new Date(dateStr).toLocaleString('ru-RU', {
      timeZone: 'Europe/Moscow',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  return (
    <div className="p-6 bg-gray-900 min-h-screen">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-white">Турниры</h1>
        <div className="flex items-center gap-4">
          <label className="flex items-center gap-2 text-white cursor-pointer">
            <input
              type="checkbox"
              checked={showPast}
              onChange={(e) => setShowPast(e.target.checked)}
              className="w-4 h-4"
            />
            <span>Показать прошедшие</span>
          </label>
          <label className="flex items-center gap-2 text-white cursor-pointer">
            <input
              type="checkbox"
              checked={showArchived}
              onChange={(e) => setShowArchived(e.target.checked)}
              className="w-4 h-4"
            />
            <span>Показать архив</span>
          </label>
        </div>
      </div>
      <table className="border border-gray-500 w-full text-sm">
        <thead>
          <tr className="bg-black text-white">
            <th className="border border-gray-500 px-2 py-1 text-left">Турнир</th>
            <th className="border border-gray-500 px-2 py-1 text-left">Локация</th>
            <th className="border border-gray-500 px-2 py-1 text-left">Дата</th>
            <th className="border border-gray-500 px-2 py-1 text-left">Тип</th>
            <th className="border border-gray-500 px-2 py-1 text-left">Цена</th>
            <th className="border border-gray-500 px-2 py-1">Всего</th>
            <th className="border border-gray-500 px-2 py-1">Оплачено</th>
            <th className="border border-gray-500 px-2 py-1">Не оплачено</th>
          </tr>
        </thead>

        <tbody>
  {filteredData.map((t) => (
    <React.Fragment key={t.tournament_id}>
      <tr
        key={t.tournament_id}
        className={`text-white cursor-pointer ${
          t.archived_at !== null 
            ? 'bg-gray-700 opacity-75 hover:bg-gray-600' 
            : 'bg-gray-800 hover:bg-gray-700'
        }`}
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
        <td className="border border-gray-500 px-2 py-1">
          {t.title}
          {t.archived_at !== null && (
            <span className="ml-2 px-1.5 py-0.5 text-xs bg-orange-600 text-white rounded">
              Архив
            </span>
          )}
        </td>
        <td className="border border-gray-500 px-2 py-1">
          {t.location || <span className="text-gray-400">—</span>}
        </td>
        <td className="border border-gray-500 px-2 py-1">
          {formatMSK(t.starts_at)}
        </td>
        <td className="border border-gray-500 px-2 py-1">
          <span className={`px-1.5 py-0.5 text-xs rounded ${
            t.tournament_type === 'team' 
              ? 'bg-blue-500 text-white' 
              : 'bg-gray-600 text-white'
          }`}>
            {t.tournament_type === 'team' ? 'Парный' : 'Личный'}
          </span>
        </td>
        <td className="border border-gray-500 px-2 py-1">{t.price_rub} ₽</td>
        <td className="border border-gray-500 px-2 py-1 text-center">{t.entries_total}</td>
        <td className="border border-gray-500 px-2 py-1 text-center">{t.entries_paid}</td>
        <td className="border border-gray-500 px-2 py-1 text-center">{t.entries_pending}</td>
      </tr>

      {openTournamentId === t.tournament_id && (
        <tr key={`details-${t.tournament_id}`}>
          <td colSpan={9} className="border border-gray-500 px-2 py-3 bg-gray-800 text-white">
            <div className="font-semibold mb-2">Участники</div>

            <table className="w-full text-sm">
              <thead>
                <tr className="bg-black text-white text-left">
                  <th className="border border-gray-500 py-1 px-2">Игрок</th>
                  <th className="border border-gray-500 py-1 px-2">Статус</th>
                  <th className="border border-gray-500 py-1 px-2">Уведомление</th>
                  <th className="border border-gray-500 py-1 px-2">Добавлен</th>
                  <th className="border border-gray-500 py-1 px-2">Обновлен</th>
                  <th className="border border-gray-500 py-1 px-2">Ссылка</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((e: any) => {
                  // Определяем, актуальна ли запись (active=true или last_seen_in_source недавно)
                  const isActive = e.active !== false;
                  const isPaidButInactive = (e.payment_status === 'paid' || e.manual_paid) && !isActive;
                  
                  return (
                  <tr 
                    key={e.entry_id} 
                    className={`border-t border-gray-500 text-white ${
                      isPaidButInactive 
                        ? 'bg-gray-700 opacity-75' 
                        : isActive 
                        ? 'bg-gray-800' 
                        : 'bg-gray-900 opacity-60'
                    }`}
                  >
                    <td className="py-1 px-2 border border-gray-500">
                      {e.player_name || e.full_name}
                      {isPaidButInactive && (
                        <span className="ml-2 px-1.5 py-0.5 text-xs bg-orange-500 text-white rounded">
                          Неактуально (оплачено)
                        </span>
                      )}
                      {!isActive && !isPaidButInactive && (
                        <span className="ml-2 px-1.5 py-0.5 text-xs bg-gray-500 text-white rounded">
                          Неактуально
                        </span>
                      )}
                    </td>
                    <td className="py-1 px-2 border border-gray-500">
                      {e.payment_status}
                      {e.manual_paid && (
                        <span className="ml-2 px-1.5 py-0.5 text-xs bg-yellow-500 text-white rounded">
                          manual
                        </span>
                      )}
                      {e.paid_by_entry_id && (
                        <span className="ml-2 px-1.5 py-0.5 text-xs bg-blue-500 text-white rounded">
                          Оплачено парой
                        </span>
                      )}
                      {e.paid_for_entry_id && (
                        <span className="ml-2 px-1.5 py-0.5 text-xs bg-green-500 text-white rounded">
                          Оплатил за пару
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
                    <td className="py-1 px-2 border border-gray-500 text-xs">
                      {e.first_seen_in_source ? (
                        formatMSK(e.first_seen_in_source)
                      ) : (
                        <span className="text-gray-400">—</span>
                      )}
                    </td>
                    <td className="py-1 px-2 border border-gray-500 text-xs">
                      {e.last_seen_in_source ? (
                        formatMSK(e.last_seen_in_source)
                      ) : (
                        <span className="text-gray-400">—</span>
                      )}
                    </td>
                    <td className="py-1 px-2 border border-gray-500">
                      <div className="flex items-center gap-2 flex-wrap">
                        {isPaidButInactive && (
                          <button
                            className="px-3 py-1.5 text-xs font-medium bg-red-600 text-white rounded hover:bg-red-700 transition-colors"
                            onClick={async () => {
                              if (!confirm(`Удалить запись для "${e.player_name || e.full_name}"? Это действие нельзя отменить.`)) return;
                              
                              const res = await fetch(
                                `${apiBase}/admin/entries/${e.entry_id}`,
                                {
                                  method: "DELETE",
                                  headers: { "Content-Type": "application/json" },
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
                                let reloadQuery = supabase
                                  .from("tournaments")
                                  .select("id, title, location, starts_at, price_rub, tournament_type, active, archived_at, first_seen_in_source, last_seen_in_source");
                                
                                if (!showArchived) {
                                  reloadQuery = reloadQuery.is("archived_at", null);
                                }
                                
                                const { data: fresh } = await reloadQuery
                                  .order("starts_at", { ascending: true });
                                
                                const { data: entriesData } = await supabase
                                  .from("entries")
                                  .select("tournament_id, payment_status");
                                
                                const entriesByTournament = new Map<number, any[]>();
                                if (entriesData) {
                                  entriesData.forEach((entry: any) => {
                                    const tid = entry.tournament_id;
                                    if (!entriesByTournament.has(tid)) {
                                      entriesByTournament.set(tid, []);
                                    }
                                    entriesByTournament.get(tid)!.push(entry);
                                  });
                                }
                                
                                const processed = (fresh || []).map((t: any) => {
                                  const entries = entriesByTournament.get(t.id) || [];
                                  const paid = entries.filter((e: any) => e.payment_status === 'paid').length;
                                  const pending = entries.filter((e: any) => e.payment_status === 'pending').length;
                                  
                                  return {
                                    tournament_id: t.id,
                                    title: t.title,
                                    location: t.location || null,
                                    starts_at: t.starts_at,
                                    price_rub: t.price_rub,
                                    tournament_type: t.tournament_type || 'personal',
                                    active: t.active !== false,
                                    archived_at: t.archived_at,
                                    entries_total: entries.length,
                                    entries_paid: paid,
                                    entries_pending: pending
                                  } as Tournament;
                                });
                                
                                setData(processed);
                              } else {
                                alert(`Ошибка: ${json.error || "неизвестная ошибка"}`);
                              }
                            }}
                          >
                            Удалить
                          </button>
                        )}
                        {e.payment_status === "paid" && isActive ? (
                          <span className="text-green-400 text-xs">Оплачено</span>
                        ) : (
                          <>
                            {e.tournament_type === 'team' ? (
                              <>
                                <button
                                  className="px-3 py-1.5 text-xs font-medium bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
                                  onClick={async () => {
                                    try {
                                      const permanentLink = `${apiBase}/p/e/${e.entry_id}?pay=half`;
                                      await navigator.clipboard.writeText(permanentLink);
                                      setCopiedEntryId(`${e.entry_id}-half`);
                                      setTimeout(() => setCopiedEntryId(null), 2000);
                                    } catch (err) {
                                      alert("Не удалось скопировать ссылку");
                                    }
                                  }}
                                >
                                  Ссылка 50%
                                </button>
                                <button
                                  className="px-3 py-1.5 text-xs font-medium bg-green-600 text-white rounded hover:bg-green-700 transition-colors"
                                  onClick={async () => {
                                    try {
                                      const permanentLink = `${apiBase}/p/e/${e.entry_id}?pay=full`;
                                      await navigator.clipboard.writeText(permanentLink);
                                      setCopiedEntryId(`${e.entry_id}-full`);
                                      setTimeout(() => setCopiedEntryId(null), 2000);
                                    } catch (err) {
                                      alert("Не удалось скопировать ссылку");
                                    }
                                  }}
                                >
                                  Ссылка 100%
                                </button>
                                {(copiedEntryId === `${e.entry_id}-half` || copiedEntryId === `${e.entry_id}-full`) && (
                                  <span className="text-xs text-green-400">Скопировано!</span>
                                )}
                              </>
                            ) : (
                              <>
                                <button
                                  className="px-3 py-1.5 text-xs font-medium bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
                                  onClick={async () => {
                                    try {
                                      const permanentLink = `${apiBase}/p/e/${e.entry_id}`;
                                      await navigator.clipboard.writeText(permanentLink);
                                      setCopiedEntryId(e.entry_id);
                                      setTimeout(() => setCopiedEntryId(null), 2000);
                                    } catch (err) {
                                      alert("Не удалось скопировать ссылку");
                                    }
                                  }}
                                >
                                  Ссылка
                                </button>
                                {copiedEntryId === String(e.entry_id) && (
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
                                    let reloadQuery = supabase
                                      .from("tournaments")
                                      .select("id, title, location, starts_at, price_rub, tournament_type, active, archived_at, first_seen_in_source, last_seen_in_source");
                                    
                                    if (!showArchived) {
                                      reloadQuery = reloadQuery.is("archived_at", null);
                                    }
                                    
                                    const { data: fresh } = await reloadQuery
                                      .order("starts_at", { ascending: true });
                                    
                                    // Load entries separately to count paid/pending
                                    const { data: entriesData } = await supabase
                                      .from("entries")
                                      .select("tournament_id, payment_status");
                                    
                                    const entriesByTournament = new Map<number, any[]>();
                                    if (entriesData) {
                                      entriesData.forEach((entry: any) => {
                                        const tid = entry.tournament_id;
                                        if (!entriesByTournament.has(tid)) {
                                          entriesByTournament.set(tid, []);
                                        }
                                        entriesByTournament.get(tid)!.push(entry);
                                      });
                                    }
                                    
                                    const processed = (fresh || []).map((t: any) => {
                                      const entries = entriesByTournament.get(t.id) || [];
                                      const paid = entries.filter((e: any) => e.payment_status === 'paid').length;
                                      const pending = entries.filter((e: any) => e.payment_status === 'pending').length;
                                      
                                      return {
                                        tournament_id: t.id,
                                        title: t.title,
                                        location: t.location || null,
                                        starts_at: t.starts_at,
                                        price_rub: t.price_rub,
                                        tournament_type: t.tournament_type || 'personal',
                                        active: t.active !== false,
                                        archived_at: t.archived_at,
                                        entries_total: entries.length,
                                        entries_paid: paid,
                                        entries_pending: pending
                                      } as Tournament;
                                    });
                                    
                                    setData(processed);
                                  } else {
                                    alert(`Ошибка: ${json.error || "неизвестная ошибка"}`);
                                  }
                                }}
                              >
                                Отметить оплату (вручную)
                              </button>
                            )}
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                  );
                })}

                {entries.length === 0 && (
                  <tr>
                    <td colSpan={5} className="py-2 px-2 border border-gray-500 text-gray-400">
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