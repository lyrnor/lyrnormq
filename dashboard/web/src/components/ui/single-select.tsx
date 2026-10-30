import { useCallback, useState, type ComponentProps } from "react";
import { DropdownMenu } from "radix-ui";
import { CheckIcon, ChevronDownIcon } from "lucide-react";
import { cn } from "@/lib/utils";

interface SingleSelectProps extends Omit<
  ComponentProps<"button">,
  "value" | "onChange" | "children"
> {
  value: string;
  onValueChange: (value: string) => void;
  options: readonly { value: string; label: string; disabled?: boolean }[];
}

/** Non-modal single-selection menu. Its portal stays in a native dialog's top layer. */
export function SingleSelect({
  value,
  onValueChange,
  options,
  className,
  disabled,
  ...props
}: SingleSelectProps) {
  const [container, setContainer] = useState<HTMLElement>();
  const setTrigger = useCallback((node: HTMLButtonElement | null) => {
    setContainer(node?.closest("dialog") ?? undefined);
  }, []);
  const selected = options.find((option) => option.value === value);
  return (
    <DropdownMenu.Root modal={false}>
      <DropdownMenu.Trigger
        {...props}
        ref={setTrigger}
        type="button"
        disabled={disabled}
        className={cn(
          "flex h-9 w-full min-w-0 items-center justify-between gap-3 rounded-md border border-input bg-background px-3 text-sm shadow-xs outline-none transition-[color,box-shadow] hover:bg-muted/50 focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:pointer-events-none disabled:opacity-50",
          className,
        )}
      >
        <span className="truncate">{selected?.label ?? "请选择"}</span>
        <ChevronDownIcon
          className="size-4 shrink-0 text-muted-foreground"
          aria-hidden="true"
        />
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal container={container}>
        <DropdownMenu.Content
          align="start"
          sideOffset={4}
          collisionPadding={8}
          className="z-50 max-h-[var(--radix-dropdown-menu-content-available-height)] w-[var(--radix-dropdown-menu-trigger-width)] min-w-32 overflow-y-auto rounded-md border bg-popover p-1 text-popover-foreground shadow-md outline-none"
        >
          <DropdownMenu.RadioGroup value={value} onValueChange={onValueChange}>
            {options.map((option) => (
              <DropdownMenu.RadioItem
                key={option.value}
                value={option.value}
                disabled={option.disabled}
                className="relative flex cursor-default items-center rounded-sm py-2 pr-3 pl-8 text-sm outline-none select-none data-[highlighted]:bg-accent data-[highlighted]:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50"
              >
                <span className="absolute left-2 flex size-4 items-center justify-center">
                  <DropdownMenu.ItemIndicator>
                    <CheckIcon className="size-4" />
                  </DropdownMenu.ItemIndicator>
                </span>
                {option.label}
              </DropdownMenu.RadioItem>
            ))}
          </DropdownMenu.RadioGroup>
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}
