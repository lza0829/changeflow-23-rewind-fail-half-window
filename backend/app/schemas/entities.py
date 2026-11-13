from pydantic import BaseModel


class ApplyRequest(BaseModel):
    stream_id: int


class ReplayCreateRequest(BaseModel):
    stream_id: int
    target_cursor: int
    # 必填：Pydantic 层直接拒绝“无上界整库重放”，服务层再防御性校验一次
    window_end_seq: int
    # 可选演示/测试用：重放至该 seq 时注入失败，验证可恢复点语义
    fail_at_seq: int | None = None


class ReplayPreviewRequest(BaseModel):
    stream_id: int
    target_cursor: int
    window_end_seq: int | None = None


class StreamPatch(BaseModel):
    enabled: bool | None = None


class SettingPut(BaseModel):
    value: str
