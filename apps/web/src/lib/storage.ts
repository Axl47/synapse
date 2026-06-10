import { Debouncer } from "@tanstack/react-pacer";
import type {
  PersistStorage,
  StateStorage as ZustandStateStorage,
  StorageValue,
} from "zustand/middleware";

export interface StateStorage<R = unknown> {
  getItem: (name: string) => string | null | Promise<string | null>;
  setItem: (name: string, value: string) => R;
  removeItem: (name: string) => R;
}

export interface DebouncedStorage<R = unknown> extends StateStorage<R> {
  flush: () => void;
}

export interface DebouncedPersistStorage<T> extends PersistStorage<T> {
  flush: () => void;
}

export function createMemoryStorage(): StateStorage {
  const store = new Map<string, string>();
  return {
    getItem: (name) => store.get(name) ?? null,
    setItem: (name, value) => {
      store.set(name, value);
    },
    removeItem: (name) => {
      store.delete(name);
    },
  };
}

export function createDebouncedStorage(
  baseStorage: StateStorage,
  debounceMs: number = 300,
): DebouncedStorage {
  const debouncedSetItem = new Debouncer(
    (name: string, value: string) => {
      baseStorage.setItem(name, value);
    },
    { wait: debounceMs },
  );

  return {
    getItem: (name) => baseStorage.getItem(name),
    setItem: (name, value) => {
      debouncedSetItem.maybeExecute(name, value);
    },
    removeItem: (name) => {
      debouncedSetItem.cancel();
      baseStorage.removeItem(name);
    },
    flush: () => {
      debouncedSetItem.flush();
    },
  };
}

export function createDebouncedJsonPersistStorage<T>(
  baseStorage: ZustandStateStorage,
  debounceMs: number = 300,
): DebouncedPersistStorage<T> {
  const debouncedSetItem = new Debouncer(
    (name: string, value: StorageValue<T>) => {
      baseStorage.setItem(name, JSON.stringify(value));
    },
    { wait: debounceMs },
  );

  return {
    getItem: (name) => {
      const rawValue = baseStorage.getItem(name);
      if (rawValue instanceof Promise) {
        return rawValue.then((value) => (value ? (JSON.parse(value) as StorageValue<T>) : null));
      }
      return rawValue ? (JSON.parse(rawValue) as StorageValue<T>) : null;
    },
    setItem: (name, value) => {
      debouncedSetItem.maybeExecute(name, value);
    },
    removeItem: (name) => {
      debouncedSetItem.cancel();
      baseStorage.removeItem(name);
    },
    flush: () => {
      debouncedSetItem.flush();
    },
  };
}
