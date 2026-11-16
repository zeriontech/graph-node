import { TestEvent } from "../generated/Contract/Contract";
import { PositionUpdateTrigger } from "../generated/schema";

export function handleTestEvent(event: TestEvent): void {
  ensureUpdateTrigger(event.params.testCommand);
}

function ensureUpdateTrigger(id: string): void {
  if (PositionUpdateTrigger.load(id) == null) {
    let trigger = new PositionUpdateTrigger(id);
    trigger.user = id;
    trigger.updateTrigger = "position";
    trigger.save();
  }
}
