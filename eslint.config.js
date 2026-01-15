import tailwindcss from "eslint-plugin-tailwindcss";

export default [
  {
    plugins: {
      tailwindcss,
    },
    rules: {
      // 🔕 Tailwind canonical / suggestion warnings OFF
      "tailwindcss/suggestCanonicalClasses": "off",

      // нэмэлт (хэрэггүй бол OFF байж болно)
      "tailwindcss/classnames-order": "off",
      "tailwindcss/no-custom-classname": "off",
    },
  },
];
