"use client";

import { useSyncExternalStore } from "react";

// Single source of truth for Country across UI pages.
// - Stored in localStorage
// - Notifies listeners via both `storage` (multi-tab) and a custom event (same-tab)

export const COUNTRIES = [
  "United States",
  "United Kingdom",
  "Germany",
  "France",
  "Canada",
  "Australia",
  "Japan",
  "South Korea",
  "Singapore",
  "India",
  "Mongolia",
  "Other",
] as const;

export type Country = (typeof COUNTRIES)[number];

const STORAGE_KEY = "goodpdf_country";
const CUSTOM_EVENT = "goodpdf:country";

export function loadCountry(): Country {
  if (typeof window === "undefined") return "Mongolia";
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) return "Mongolia";
  return (COUNTRIES as readonly string[]).includes(raw) ? (raw as Country) : "Mongolia";
}

export function saveCountry(country: Country) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(STORAGE_KEY, country);
  // same-tab notification
  window.dispatchEvent(new Event(CUSTOM_EVENT));
}

function subscribe(onStoreChange: () => void) {
  if (typeof window === "undefined") return () => {};

  const onStorage = (e: StorageEvent) => {
    if (e.key === STORAGE_KEY) onStoreChange();
  };

  const onCustom = () => onStoreChange();

  window.addEventListener("storage", onStorage);
  window.addEventListener(CUSTOM_EVENT, onCustom);

  return () => {
    window.removeEventListener("storage", onStorage);
    window.removeEventListener(CUSTOM_EVENT, onCustom);
  };
}

export function useCountry(): Country {
  return useSyncExternalStore(subscribe, loadCountry, () => "Mongolia");
}
