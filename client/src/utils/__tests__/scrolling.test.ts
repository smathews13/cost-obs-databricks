import { readFileSync } from "node:fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  installTransientScrollbarBehavior,
  lockDocumentScroll,
} from "../scrolling";

describe("shared scrolling behavior", () => {
  let originalInnerWidth: number;
  let originalClientWidth: PropertyDescriptor | undefined;

  beforeEach(() => {
    originalInnerWidth = window.innerWidth;
    originalClientWidth = Object.getOwnPropertyDescriptor(
      document.documentElement,
      "clientWidth",
    );
    document.body.removeAttribute("style");
    delete document.body.dataset.scrollLocked;
    delete document.body.dataset.scrollLockCount;
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    Object.defineProperty(window, "innerWidth", {
      configurable: true,
      value: originalInnerWidth,
    });
    if (originalClientWidth) {
      Object.defineProperty(
        document.documentElement,
        "clientWidth",
        originalClientWidth,
      );
    } else {
      delete (document.documentElement as HTMLElement & { clientWidth?: number }).clientWidth;
    }
    document.body.removeAttribute("style");
    delete document.body.dataset.scrollLocked;
    delete document.body.dataset.scrollLockCount;
  });

  it("reference-counts nested locks and compensates for a missing gutter", () => {
    vi.stubGlobal("CSS", { supports: () => false });
    Object.defineProperty(window, "innerWidth", {
      configurable: true,
      value: 1000,
    });
    Object.defineProperty(document.documentElement, "clientWidth", {
      configurable: true,
      value: 980,
    });
    document.body.style.overflow = "auto";
    document.body.style.paddingRight = "4px";

    const unlockOuter = lockDocumentScroll();
    const unlockInner = lockDocumentScroll();

    expect(document.body.style.overflow).toBe("hidden");
    expect(document.body.style.paddingRight).toBe("24px");
    expect(document.body.dataset.scrollLockCount).toBe("2");

    unlockInner();
    expect(document.body.style.overflow).toBe("hidden");
    expect(document.body.dataset.scrollLockCount).toBe("1");

    unlockOuter();
    unlockOuter();
    expect(document.body.style.overflow).toBe("auto");
    expect(document.body.style.paddingRight).toBe("4px");
    expect(document.body).not.toHaveAttribute("data-scroll-locked");
  });

  it("uses the stable root gutter without adding body padding", () => {
    vi.stubGlobal("CSS", { supports: (value: string) => value === "scrollbar-gutter: stable" });
    Object.defineProperty(window, "innerWidth", {
      configurable: true,
      value: 1000,
    });
    Object.defineProperty(document.documentElement, "clientWidth", {
      configurable: true,
      value: 980,
    });
    document.body.style.paddingRight = "3px";

    const unlock = lockDocumentScroll();
    expect(document.body.style.paddingRight).toBe("3px");
    unlock();
  });

  it("adds transient classes while scrolling or hovering at the scrollbar edge", () => {
    vi.useFakeTimers();
    const scroller = document.createElement("div");
    document.body.append(scroller);
    Object.defineProperties(scroller, {
      clientHeight: { configurable: true, value: 100 },
      scrollHeight: { configurable: true, value: 300 },
    });
    scroller.getBoundingClientRect = () => ({
      bottom: 100,
      height: 100,
      left: 0,
      right: 200,
      top: 0,
      width: 200,
      x: 0,
      y: 0,
      toJSON: () => ({}),
    });
    const uninstall = installTransientScrollbarBehavior();

    scroller.dispatchEvent(new Event("scroll"));
    expect(scroller).toHaveClass("is-scroll-active");
    vi.advanceTimersByTime(701);
    expect(scroller).not.toHaveClass("is-scroll-active");

    scroller.dispatchEvent(new MouseEvent("pointermove", {
      bubbles: true,
      clientX: 195,
    }));
    expect(scroller).toHaveClass("is-scrollbar-edge-hover");
    scroller.dispatchEvent(new MouseEvent("pointermove", {
      bubbles: true,
      clientX: 100,
    }));
    expect(scroller).not.toHaveClass("is-scrollbar-edge-hover");

    scroller.dispatchEvent(new Event("scroll"));
    uninstall();
    expect(scroller).not.toHaveClass("is-scroll-active");
    scroller.remove();
  });

  it("keeps the root gutter stable and scrollbar thumbs hidden at rest", () => {
    const css = readFileSync("src/index.css", "utf8");
    expect(css).toMatch(/html\s*\{[^}]*scrollbar-gutter:\s*stable/s);
    expect(css).toMatch(/\*\s*\{[^}]*scrollbar-color:\s*transparent transparent/s);
    expect(css).toMatch(/\.is-scroll-active,[\s\S]*\.is-scrollbar-edge-hover,[\s\S]*:focus-within/);
    expect(css).toMatch(/::-webkit-scrollbar-thumb\s*\{[^}]*background:\s*transparent/s);
    expect(css).toMatch(
      /@media \(prefers-reduced-motion: reduce\)\s*\{\s*html\s*\{\s*scroll-behavior:\s*auto/s,
    );
  });
});
