# How DataTransfer Enables Safe File Replacement

Browsers do not allow directly mutating a `FileList` (for example, `input.files[0] = newFile`) because selected files are security-sensitive.

The `DataTransfer` API gives us a controlled way to build a **new** `FileList`:

1. Create a new `DataTransfer` object.
2. Add desired `File` objects with `dataTransfer.items.add(file)`.
3. Assign `input.files = dataTransfer.files`.
4. Dispatch a synthetic `change` event so the host page reacts as if the user picked those files.

In this extension, we intercept the original PDF, sanitize it via backend, create a new `File` for the sanitized PDF, rebuild `FileList` through `DataTransfer`, and re-emit `change`.

This pattern works within browser security constraints because we are not mutating the old `FileList`; we are replacing it with a newly constructed one.
