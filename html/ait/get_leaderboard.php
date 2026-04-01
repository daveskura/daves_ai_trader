
<?php
header('Content-Type: application/json');

// DB connection
$host = "localhost";
$user = "aitrader";
$password = "aitrader";
$dbname = "aitrader_db";

$conn = new mysqli($host, $user, $password, $dbname);

if ($conn->connect_error) {
    http_response_code(500);
    echo json_encode(["error" => "DB connection failed"]);
    exit;
}

// Get latest leaderboard date
$dateQuery = "SELECT MAX(lb_date) as latest_date FROM leaderboard";
$dateResult = $conn->query($dateQuery);
$row = $dateResult->fetch_assoc();
$latestDate = $row['latest_date'];

// Pull leaderboard for latest date
$sql = "
SELECT 
    lb_date as date,
    rank_pos,
    strategy_id,
    strategy_name,
    style,
    risk,
    cash,
    holdings_value,
    total,
    pnl,
    pct_return,
    trades
FROM leaderboard
WHERE lb_date = ?
ORDER BY total DESC
";

$stmt = $conn->prepare($sql);
$stmt->bind_param("s", $latestDate);
$stmt->execute();
$result = $stmt->get_result();

$data = [];
while ($r = $result->fetch_assoc()) {
    $data[] = $r;
}

echo json_encode($data);

$conn->close();
?>