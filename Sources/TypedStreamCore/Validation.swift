/// Semantic validation: constrained decoding guarantees *shape*,
/// a validator guarantees *meaning* (e.g. `endDate > startDate`).
///
/// Throw `StructuredOutputError.constraintViolated(path:reason:)` or
/// `StructuredOutputError.constraintsViolated(_:)` on failure —
/// the error text becomes the repair signal, so good messages are a feature.
/// Any other thrown error is wrapped as `.undecodable` and enters the bounded
/// repair loop. Only `CancellationError` escapes — it is never repaired.
public typealias Validator<Output: Sendable> = @Sendable (Output) throws -> Void

/// Extracts a human-readable property path from a `KeyPath` (e.g. `\Movie.year`
/// -> `"year"`, `\Movie.director.name` -> `"director.name"`).
internal func formattedKeyPath<Root, Value>(_ keyPath: KeyPath<Root, Value>) -> String {
    var raw = String(describing: keyPath)
    if raw.hasPrefix("\\") {
        raw.removeFirst()
    }
    let rootName = String(describing: Root.self)
    if raw == rootName {
        return "self"
    }
    if raw.hasPrefix(rootName + ".") {
        return String(raw.dropFirst(rootName.count + 1))
    }
    if let range = raw.range(of: "." + rootName + ".") {
        return String(raw[range.upperBound...])
    }
    if let dotIndex = raw.firstIndex(of: ".") {
        let afterDot = raw.index(after: dotIndex)
        if afterDot < raw.endIndex {
            return String(raw[afterDot...])
        }
    }
    return raw
}

/// A composable semantic validation rule over `Output` that collects all
/// ``ConstraintViolation``s in a single pass instead of short-circuiting on the
/// first failure.
///
/// Collecting all violations in one pass lets the repair prompt report every
/// broken constraint simultaneously, avoiding multi-round "whack-a-mole"
/// retries.
public struct Check<Output: Sendable>: Sendable {
    private let evaluator: @Sendable (Output) -> [ConstraintViolation]

    /// Creates a rule from a custom multi-violation evaluator.
    public init(evaluate: @Sendable @escaping (Output) -> [ConstraintViolation]) {
        self.evaluator = evaluate
    }

    /// Creates a whole-value rule (e.g. cross-field invariant `endDate > startDate`).
    public init(
        path: String = "root",
        reason: String,
        predicate: @Sendable @escaping (Output) -> Bool
    ) {
        self.evaluator = { output in
            predicate(output) ? [] : [ConstraintViolation(path: path, reason: reason)]
        }
    }

    /// Creates a whole-value rule with a dynamic failure message derived from the value.
    public init(
        path: String = "root",
        reason: @Sendable @escaping (Output) -> String,
        predicate: @Sendable @escaping (Output) -> Bool
    ) {
        self.evaluator = { output in
            predicate(output) ? [] : [ConstraintViolation(path: path, reason: reason(output))]
        }
    }

    /// Creates a key-path rule over a specific property on `Output`.
    ///
    /// If `path` is omitted, the property name is derived from `keyPath`.
    public init<Value: Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        path: String? = nil,
        reason: String,
        predicate: @Sendable @escaping (Value) -> Bool
    ) {
        let resolvedPath = path ?? formattedKeyPath(keyPath)
        self.evaluator = { output in
            let value = output[keyPath: keyPath]
            return predicate(value)
                ? []
                : [ConstraintViolation(path: resolvedPath, reason: reason)]
        }
    }

    /// Creates a key-path rule with a dynamic failure message derived from the property value.
    ///
    /// If `path` is omitted, the property name is derived from `keyPath`.
    public init<Value: Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        path: String? = nil,
        reason: @Sendable @escaping (Value) -> String,
        predicate: @Sendable @escaping (Value) -> Bool
    ) {
        let resolvedPath = path ?? formattedKeyPath(keyPath)
        self.evaluator = { output in
            let value = output[keyPath: keyPath]
            return predicate(value)
                ? []
                : [ConstraintViolation(path: resolvedPath, reason: reason(value))]
        }
    }

    /// Evaluates `output` and returns all violations found in a single pass.
    public func evaluate(_ output: Output) -> [ConstraintViolation] {
        evaluator(output)
    }

    /// Validates `output`, throwing ``StructuredOutputError/constraintsViolated(_:)``
    /// if one or more constraints fail.
    public func validate(_ output: Output) throws {
        let violations = evaluate(output)
        guard violations.isEmpty else {
            throw StructuredOutputError.constraintsViolated(violations)
        }
    }

    /// Allows a `Check<Output>` to be called directly as a throwing validator.
    public func callAsFunction(_ output: Output) throws {
        try validate(output)
    }

    /// Converts this `Check` into a pipeline ``Validator``.
    public var validator: Validator<Output> {
        { output in try self.validate(output) }
    }

    // MARK: - Composition

    /// Returns a new `Check` that runs both `self` and `other`, collecting all
    /// violations from both in one pass.
    public func combined(with other: Check<Output>) -> Check<Output> {
        Check { output in
            self.evaluate(output) + other.evaluate(output)
        }
    }

    /// Combines an array of `Check` rules into a single rule that evaluates all
    /// of them in one pass and concatenates their violations in order.
    public static func all(_ checks: [Check<Output>]) -> Check<Output> {
        Check { output in
            checks.flatMap { $0.evaluate(output) }
        }
    }

    /// Combines multiple `Check` rules into a single rule that evaluates all
    /// of them in one pass and concatenates their violations in order.
    public static func all(_ checks: Check<Output>...) -> Check<Output> {
        all(checks)
    }

    /// Non-short-circuiting conjunction: evaluates both `lhs` and `rhs` and
    /// collects all violations from both in a single pass.
    public static func && (lhs: Check<Output>, rhs: Check<Output>) -> Check<Output> {
        lhs.combined(with: rhs)
    }

    /// Builds a throwing ``Validator`` that runs all `checks` in one pass.
    public static func validator(_ checks: [Check<Output>]) -> Validator<Output> {
        all(checks).validator
    }

    /// Builds a throwing ``Validator`` that runs all `checks` in one pass.
    public static func validator(_ checks: Check<Output>...) -> Validator<Output> {
        all(checks).validator
    }

    // MARK: - Built-in KeyPath Rules

    /// Asserts that a collection property (such as `String` or `[Element]`) is not empty.
    public static func nonEmpty<Value: Collection & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        path: String? = nil,
        reason: String = "must not be empty"
    ) -> Check<Output> {
        Check(keyPath, path: path, reason: reason) { !$0.isEmpty }
    }

    /// Asserts that a `String` property contains at least one non-whitespace character.
    public static func notBlank(
        _ keyPath: KeyPath<Output, String> & Sendable,
        path: String? = nil,
        reason: String = "must not be blank"
    ) -> Check<Output> {
        Check(keyPath, path: path, reason: reason) { value in
            value.contains { !$0.isWhitespace && !$0.isNewline }
        }
    }

    /// Asserts that a comparable property lies within `range`.
    public static func inRange<Value: Comparable & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        _ range: ClosedRange<Value>,
        path: String? = nil,
        reason: String? = nil
    ) -> Check<Output> {
        let message = reason ?? "must be in \(range.lowerBound)...\(range.upperBound)"
        return Check(keyPath, path: path, reason: message) { range.contains($0) }
    }

    /// Asserts that a comparable property is `>= minimum`.
    public static func greaterThanOrEqual<Value: Comparable & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        _ minimum: Value,
        path: String? = nil,
        reason: String? = nil
    ) -> Check<Output> {
        let message = reason ?? "must be >= \(minimum)"
        return Check(keyPath, path: path, reason: message) { $0 >= minimum }
    }

    /// Asserts that a comparable property is `<= maximum`.
    public static func lessThanOrEqual<Value: Comparable & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        _ maximum: Value,
        path: String? = nil,
        reason: String? = nil
    ) -> Check<Output> {
        let message = reason ?? "must be <= \(maximum)"
        return Check(keyPath, path: path, reason: message) { $0 <= maximum }
    }

    /// Asserts that a comparable property is `> minimum`.
    public static func greaterThan<Value: Comparable & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        _ minimum: Value,
        path: String? = nil,
        reason: String? = nil
    ) -> Check<Output> {
        let message = reason ?? "must be > \(minimum)"
        return Check(keyPath, path: path, reason: message) { $0 > minimum }
    }

    /// Asserts that a comparable property is `< maximum`.
    public static func lessThan<Value: Comparable & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        _ maximum: Value,
        path: String? = nil,
        reason: String? = nil
    ) -> Check<Output> {
        let message = reason ?? "must be < \(maximum)"
        return Check(keyPath, path: path, reason: message) { $0 < maximum }
    }

    /// Asserts that an equatable property equals `expected`.
    public static func equals<Value: Equatable & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        _ expected: Value,
        path: String? = nil,
        reason: String? = nil
    ) -> Check<Output> {
        let message = reason ?? "must equal \(expected)"
        return Check(keyPath, path: path, reason: message) { $0 == expected }
    }

    /// Asserts that a hashable property is a member of `allowed`.
    public static func oneOf<Value: Hashable & Sendable>(
        _ keyPath: KeyPath<Output, Value> & Sendable,
        _ allowed: Set<Value>,
        path: String? = nil,
        reason: String? = nil
    ) -> Check<Output> {
        let message = reason ?? "must be one of the allowed values"
        return Check(keyPath, path: path, reason: message) { allowed.contains($0) }
    }

    /// Scopes a child `Check` onto a nested property, prefixing child violation
    /// paths with the parent property path.
    public static func nested<Child: Sendable>(
        _ keyPath: KeyPath<Output, Child> & Sendable,
        path: String? = nil,
        _ childCheck: Check<Child>
    ) -> Check<Output> {
        let prefix = path ?? formattedKeyPath(keyPath)
        return Check { output in
            let child = output[keyPath: keyPath]
            return childCheck.evaluate(child).map { violation in
                let combinedPath = violation.path == "root" || violation.path.isEmpty
                    ? prefix
                    : "\(prefix).\(violation.path)"
                return ConstraintViolation(path: combinedPath, reason: violation.reason)
            }
        }
    }

    /// Scopes an element `Check` across every element of an array property,
    /// prefixing each violation path with `"\(path)[\(index)]"`.
    public static func forEach<Element: Sendable>(
        _ keyPath: KeyPath<Output, [Element]> & Sendable,
        path: String? = nil,
        _ elementCheck: Check<Element>
    ) -> Check<Output> {
        let prefix = path ?? formattedKeyPath(keyPath)
        return Check { output in
            let elements = output[keyPath: keyPath]
            var violations: [ConstraintViolation] = []
            for (index, element) in elements.enumerated() {
                for violation in elementCheck.evaluate(element) {
                    let indexedPath = violation.path == "root" || violation.path.isEmpty
                        ? "\(prefix)[\(index)]"
                        : "\(prefix)[\(index)].\(violation.path)"
                    violations.append(ConstraintViolation(path: indexedPath, reason: violation.reason))
                }
            }
            return violations
        }
    }
}
