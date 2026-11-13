from fastapi import APIRouter, HTTPException

from app.schemas.entities import ReplayCreateRequest, ReplayPreviewRequest
from app.services import replay_service

router = APIRouter(tags=["replay"])


@router.get("/replay/tasks")
def list_replay_tasks():
    # 任务持久化在 replay_tasks 表：刷新页面后历史与进度仍在
    return replay_service.list_replay_tasks()


@router.get("/replay/tasks/{task_id}")
def get_replay_task(task_id: int):
    task = replay_service.get_replay_task(task_id)
    if not task:
        raise HTTPException(404, "replay task not found")
    return task


@router.post("/replay/preview")
def preview_replay(body: ReplayPreviewRequest):
    try:
        return replay_service.preview_window(
            body.stream_id, body.target_cursor, body.window_end_seq
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/replay/tasks")
def create_replay_task(body: ReplayCreateRequest):
    try:
        return replay_service.create_and_run_replay(
            stream_id=body.stream_id,
            target_cursor=body.target_cursor,
            window_end_seq=body.window_end_seq,
            fail_at_seq=body.fail_at_seq,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
