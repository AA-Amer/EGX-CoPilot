import { useState } from "react";
import Sidebar from "./components/Sidebar";
import Chat from "./components/Chat";
import SwingPanel from "./components/SwingPanel";
import SettingsPage from "./components/SettingsPage";

export default function App() {
  const [view, setView] = useState("chat"); // "chat" | "swing" | "settings"

  return (
    <div style={{ display: "flex", height: "100vh" }}>
      <Sidebar view={view} setView={setView} />
      <main style={{ flex: 1, overflow: "auto" }}>
        {view === "chat" && <Chat />}
        {view === "swing" && <SwingPanel />}
        {view === "settings" && <SettingsPage />}
      </main>
    </div>
  );
}
