"use client";

import { useEffect, useState } from "react";

/* =========================
   Types
========================= */
export type Country =
  | "United States"
  | "United Kingdom"
  | "Germany"
  | "France"
  | "Canada"
  | "Australia"
  | "Japan"
  | "South Korea"
  | "Singapore"
  | "India"
  | "Mongolia"
  | "Other";

/* =========================
   Storage keys
========================= */
const COUNTRY_KEY = "goodpdf_country";
const LOCK_KEY = "goodpdf_country_locked";

/* =========================
   Defaults
========================= */
const DEFAULT_COUNTRY: Country = "Mongolia";

/* =========================
   Low-level helpers
========================= */
export function loadCountry(): Country {
  if (typeof window === "undefined") return DEFAULT_COUNTRY;
  const v = window.localStorage.getItem(COUNTRY_KEY) as Country | null;
  return v ?? DEFAULT_COUNTRY;
}

export function saveCountry(country: Country) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(COUNTRY_KEY, country);
  // same-tab UI refresh
  window.dispatchEvent(new Event("goodpdf:country"));
}

/* =========================
   Lock logic (1-time choice)
========================= */
export function isCountryLocked(): boolean {
  if (typeof window === "undefined") return false;
  return window.localStorage.getItem(LOCK_KEY) === "1";
}

export function lockCountry() {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(LOCK_KEY, "1");
  window.dispatchEvent(new Event("goodpdf:country"));
}

/**
 * Country-г ЗӨВХӨН 1 удаа тохируулна.
 * Locked бол дараагийн оролдлогыг үл тооно.
 */
export function setCountryOnce(country: Country) {
  if (isCountryLocked()) return;
  saveCountry(country);
  lockCountry();
}

/* =========================
   React hook (single source)
========================= */
export function useCountry(): Country {
  const [country, setCountry] = useState<Country>(DEFAULT_COUNTRY);

  useEffect(() => {
    // initial load
    setCountry(loadCountry());

    const onStorage = (e: StorageEvent) => {
      if (e.key === COUNTRY_KEY) {
        setCountry(loadCountry());
      }
    };

    const onLocal = () => {
      setCountry(loadCountry());
    };

    window.addEventListener("storage", onStorage);
    window.addEventListener("goodpdf:country", onLocal);

    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener("goodpdf:country", onLocal);
    };
  }, []);

  return country;
}
