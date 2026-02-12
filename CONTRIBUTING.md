# Contributing to Orca

Thank you for considering contributing to Orca! 🐳

We welcome contributions from everyone — developers, data scientists, and enthusiasts alike.

## 💡 How to Contribute

1. **Fork the repository**

Click the **Fork** button on GitHub and clone your fork:

```bash
git clone https://github.com/yourusername/core.git
cd orca
```

2. **Create a new branch**

```bash
git checkout -b feature/your-feature-name
```

3. **Make your changes**

Follow code style, write tests if needed, and make sure everything builds.

4. **Commit your changes**

```bash
git commit -m "Describe your change"
```

5. **Push to your fork**

```bash
git push origin feature/your-feature-name
```

6. **Open a Pull Request**

Go to the main Orca repo and open a PR. Describe your change clearly.

---

## 📋 Code Guidelines

- Keep PRs focused on a single topic.
- Follow Go best practices.
- Write clear commit messages.
- Add tests or examples when possible.

---

## 📦 Local Setup

- Use `make build_store` to start the DB.
- Use `make build` to build the Orca binary.
- Run tests with `go test ./...`

---

## 🏗️ Issues and Feature Requests

- Report bugs or request features in [GitHub Issues](https://github.com/orca-telemetry/orca/issues).
- Check for existing issues before creating a new one.

---

## 📃 License

By contributing, you agree that your contributions will be licensed under the [BSL](LICENSE).

---

Thank you for helping improve Orca! 💙
