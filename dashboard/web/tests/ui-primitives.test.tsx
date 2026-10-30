import React, { useState } from "react";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Checkbox } from "../src/components/ui/checkbox";
import { Switch } from "../src/components/ui/switch";
import { Field } from "../src/components/ui/field";
import { Label } from "../src/components/ui/label";
import { Textarea } from "../src/components/ui/textarea";
import { Alert, AlertDescription } from "../src/components/ui/alert";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "../src/components/ui/tabs";

afterEach(cleanup);
describe("shared form components", () => {
  it("associates an automatically identified checkbox with its label and toggles controlled state", () => {
    function Example() {
      const [checked, setChecked] = useState(false);
      return (
        <Field label="绑定出站">
          <Checkbox
            checked={checked}
            onCheckedChange={(value) => setChecked(value === true)}
          />
        </Field>
      );
    }
    render(<Example />);
    const checkbox = screen.getByRole("checkbox", { name: "绑定出站" });
    expect(checkbox.getAttribute("aria-checked")).toBe("false");
    fireEvent.click(screen.getByText("绑定出站"));
    expect(checkbox.getAttribute("aria-checked")).toBe("true");
    fireEvent.click(checkbox);
    expect(checkbox.getAttribute("aria-checked")).toBe("false");
  });
  it("renders a controlled switch without allowing disabled interaction or form submission", () => {
    const change = vi.fn();
    render(
      <>
        <Label htmlFor="enabled">启用</Label>
        <Switch id="enabled" checked disabled onCheckedChange={change} />
      </>,
    );
    const control = screen.getByRole("switch", { name: "启用" });
    expect(control.getAttribute("aria-checked")).toBe("true");
    expect(control.getAttribute("type")).toBe("button");
    fireEvent.click(control);
    fireEvent.keyDown(control, { key: " " });
    expect(change).not.toHaveBeenCalled();
  });
  it("updates controlled switch state and permits direct label activation", () => {
    function Example() {
      const [checked, setChecked] = useState(true);
      return (
        <>
          <Label htmlFor="active">启用规则</Label>
          <Switch id="active" checked={checked} onCheckedChange={setChecked} />
        </>
      );
    }
    render(<Example />);
    const control = screen.getByRole("switch", { name: "启用规则" });
    fireEvent.click(screen.getByText("启用规则"));
    expect(control.getAttribute("aria-checked")).toBe("false");
    fireEvent.click(control);
    expect(control.getAttribute("aria-checked")).toBe("true");
  });
  it("preserves an explicit textarea identity and forwards form constraints", () => {
    const change = vi.fn();
    render(
      <Field label="SQL">
        <Textarea
          id="query"
          defaultValue="SELECT *"
          maxLength={8192}
          required
          onChange={change}
        />
      </Field>,
    );
    const control = screen.getByLabelText("SQL") as HTMLTextAreaElement;
    expect(control.id).toBe("query");
    expect(control.required).toBe(true);
    expect(control.maxLength).toBe(8192);
    fireEvent.change(control, { target: { value: "SELECT topic" } });
    expect(change).toHaveBeenCalledOnce();
  });
  it("gives repeated field labels distinct accessible control identities", () => {
    render(
      <>
        <Field label="样例">
          <Textarea />
        </Field>
        <Field label="样例">
          <Textarea />
        </Field>
      </>,
    );
    const fields = screen.getAllByLabelText("样例");
    expect(fields[0].id).not.toBe(fields[1].id);
    expect(fields.every((field) => field.id.length > 0)).toBe(true);
  });
  it("supports arrow-key tab selection and displays only the active panel", async () => {
    render(
      <Tabs defaultValue="rules">
        <TabsList aria-label="规则管理">
          <TabsTrigger value="rules">规则</TabsTrigger>
          <TabsTrigger value="outbounds">出站</TabsTrigger>
        </TabsList>
        <TabsContent value="rules">规则列表</TabsContent>
        <TabsContent value="outbounds">出站列表</TabsContent>
      </Tabs>,
    );
    const rules = screen.getByRole("tab", { name: "规则" });
    rules.focus();
    fireEvent.keyDown(rules, { key: "ArrowRight" });
    await waitFor(() =>
      expect(
        screen.getByRole("tab", { name: "出站" }).getAttribute("aria-selected"),
      ).toBe("true"),
    );
    expect(screen.getByRole("tabpanel").textContent).toBe("出站列表");
    expect(document.querySelector("style")).toBeNull();
  });
  it("supports a descriptive alert without requiring runtime styles", () => {
    render(
      <Alert variant="destructive">
        <AlertDescription>保存失败，请重试</AlertDescription>
      </Alert>,
    );
    expect(screen.getByRole("alert").textContent).toContain("保存失败");
    expect(document.querySelector("style")).toBeNull();
  });
});
