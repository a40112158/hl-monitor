import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import hl_monitor_final as monitor


class PositionAccountingV2Tests(unittest.TestCase):
    def test_small_reduction_is_accounted_without_noisy_event(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "monitor.db")
            with mock.patch.multiple(
                monitor,
                DB_FILE=db,
                USE_TURSO=False,
                POSITION_TRADE_MODE=True,
                POSITION_MIN_QTY_CHANGE_RATIO=0.05,
                POSITION_MIN_QTY_CHANGE_USD=1000.0,
            ):
                monitor.init_db()
                conn = sqlite3.connect(db)
                conn.execute("INSERT INTO runs(run_id, started_at, snapshot_complete, price_data_ok) VALUES (1, ?, 1, 1)", (monitor.now_str(),))
                conn.execute(
                    "INSERT INTO wallet_states(run_id,address,groups,status,perp_ok,spot_ok) VALUES (1,?,'smart_money','ok',1,1)",
                    ("0x" + "1" * 40,),
                )
                conn.execute(
                    """
                    INSERT INTO perp_positions(run_id,address,groups,coin,side,szi,abs_szi,entry_px,mark_px,position_value,leverage,liq_distance_pct)
                    VALUES (1,?,'smart_money','BTC','long',99,99,100,110,10890,2,50)
                    """,
                    ("0x" + "1" * 40,),
                )
                conn.execute(
                    """
                    INSERT INTO position_trades(
                        address,groups,coin,side,status,open_time,last_seen,entry_px,current_px,
                        initial_qty,current_qty,max_qty,closed_qty,closed_notional_usd,max_position_value,
                        current_position_value,avg_leverage,max_leverage,min_liq_distance_pct,
                        realized_return_pct,realized_pnl_usd,unrealized_return_pct,estimated_roe_pct,
                        final_return_pct,max_favorable_pct,max_adverse_pct,holding_hours,add_count,reduce_count
                    ) VALUES (?,'smart_money','BTC','long','open',?,?,100,100,100,100,100,0,0,10000,10000,2,2,50,0,0,0,0,0,0,0,0,0,0)
                    """,
                    ("0x" + "1" * 40, monitor.now_str(), monitor.now_str()),
                )
                conn.commit(); conn.close()

                monitor.update_position_trades(1, {"BTC": 110.0})

                conn = sqlite3.connect(db)
                conn.row_factory = sqlite3.Row
                row = dict(conn.execute("SELECT * FROM position_trades WHERE coin='BTC'").fetchone())
                event_count = conn.execute("SELECT COUNT(*) FROM position_trade_events WHERE event_type='reduce'").fetchone()[0]
                conn.close()

                self.assertAlmostEqual(row["closed_qty"], 1.0)
                self.assertAlmostEqual(row["closed_notional_usd"], 100.0)
                self.assertAlmostEqual(row["realized_pnl_usd"], 10.0)
                self.assertGreater(row["final_return_pct"], 0.0)
                self.assertEqual(event_count, 0)


if __name__ == "__main__":
    unittest.main()
