"use client";

import { useEffect, useState } from "react";
import { supabase } from "@/lib/supabase/client";

export function useAccountSummary() {
  const [loading, setLoading] = useState(true);
  const [remainingFiles, setRemainingFiles] = useState(0);
  const [daysLeft, setDaysLeft] = useState(0);

  useEffect(() => {
    (async () => {
      setLoading(true);

      // 1) Supabase session авч чадаж байна уу гэдгийг тест
      const { data, error } = await supabase.auth.getSession();

      if (error) {
        console.error(error);
        // холболт алдаатай бол эндээс мэдэгдэнэ
        setRemainingFiles(-1);
        setDaysLeft(-1);
        setLoading(false);
        return;
      }

      // session байвал days/files/cpu-г дараа нь entitlement-аас авна
      // одоогоор зөвхөн "connect OK" гэдгийг харуулахын тулд mock тоо
      const hasSession = !!data.session;
      setRemainingFiles(hasSession ? 30 : 0);
      setDaysLeft(hasSession ? 30 : 0);

      setLoading(false);
    })();
  }, []);

  return { loading, remainingFiles, daysLeft };
}
