// TODO: Navigation sidebar with wallet summary
export default function Sidebar({ view, setView }) {
  return (
    <nav style={{ width: 220, borderRight: "1px solid #eee", padding: 16 }}>
      <h2>EGX Copilot</h2>
      <ul style={{ listStyle: "none", padding: 0 }}>
        {["chat", "swing", "settings"].map((v) => (
          <li key={v}>
            <button onClick={() => setView(v)} style={{ fontWeight: view === v ? "bold" : "normal" }}>
              {v.charAt(0).toUpperCase() + v.slice(1)}
            </button>
          </li>
        ))}
      </ul>
    </nav>
  );
}
