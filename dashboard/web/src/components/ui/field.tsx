import {
  cloneElement,
  Fragment,
  isValidElement,
  useId,
  type ReactNode,
} from "react";
import { Label } from "./label";

/** Associate a visible label with one labelable control, retaining explicit IDs. */
export function Field({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  const generated = useId();
  const control =
    isValidElement<{ id?: string }>(children) && children.type !== Fragment
      ? children
      : null;
  const id = control ? control.props.id || generated : undefined;
  return (
    <div data-slot="field" className="grid gap-2">
      <Label htmlFor={id}>{label}</Label>
      {control ? cloneElement(control, { id }) : children}
    </div>
  );
}
