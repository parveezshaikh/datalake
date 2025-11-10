from pathlib import Path

from services.workers.pipeline_runner import PipelineRunner


def test_sample_pipeline_executes():
    # Run the sample pipeline against the repository data/config layout
    runner = PipelineRunner(base_path=Path.cwd())

    result = runner.run_by_id(app="sample_app", layer="staging", pipeline_id="cards_balance_snapshot", overrides={"run_id": "unit-test"})

    assert result["pipeline_id"] == "cards_balance_snapshot"
    assert result["run_id"] == "unit-test"
    assert result["metrics"]["pipeline_source_loaded"] == 1

    output_path = Path.cwd() / "data" / "applications" / "sample_app" / "service" / "cards_balance_summary.csv"
    assert output_path.exists()
    content = output_path.read_text().strip()
    assert "balance_typ" in content
