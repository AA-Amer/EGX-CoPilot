// TODO: Card showing ticker, price, change%, signal score, Shariah badge
export default function StockCard({ ticker, price, changePct, score, shariah }) {
  return (
    <div style={{ border: "1px solid #ddd", borderRadius: 8, padding: 12 }}>
      <strong>{ticker}</strong>
      <p>{price} EGP ({changePct > 0 ? "+" : ""}{changePct}%)</p>
      {score != null && <p>Signal: {score}/100</p>}
      {shariah != null && <p>{shariah ? "✓ Shariah" : "✗ Non-compliant"}</p>}
    </div>
  );
}
