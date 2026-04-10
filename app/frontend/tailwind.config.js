/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        "dbx-red": "#FF3621",
        "dbx-dark": "#1B3139",
        "dbx-gray": "#F5F5F5",
      },
    },
  },
  plugins: [],
};
