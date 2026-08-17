Since you're using **LazyVim (Neovim)** now, you don't need to learn 500 commands. Around **50 commands** will make you very productive.

I'll organize them by category.

---

# 1. Modes

| Key   | Action                      |
| ----- | --------------------------- |
| `i`   | Insert before cursor        |
| `I`   | Insert at beginning of line |
| `a`   | Insert after cursor         |
| `A`   | Insert at end of line       |
| `o`   | New line below              |
| `O`   | New line above              |
| `Esc` | Back to Normal mode         |
| `:`   | Command mode                |

---

# 2. Save & Exit

| Command | Action              |
| ------- | ------------------- |
| `:w`    | Save                |
| `:q`    | Quit                |
| `:wq`   | Save & Quit         |
| `:x`    | Save & Quit         |
| `:q!`   | Quit without saving |
| `ZZ`    | Save & Quit         |

---

# 3. Cursor Movement

| Key   | Action                    |
| ----- | ------------------------- |
| `h`   | Left                      |
| `j`   | Down                      |
| `k`   | Up                        |
| `l`   | Right                     |
| `w`   | Next word                 |
| `b`   | Previous word             |
| `e`   | End of word               |
| `0`   | Beginning of line         |
| `^`   | First non-space character |
| `$`   | End of line               |
| `gg`  | Top of file               |
| `G`   | Bottom of file            |
| `50G` | Go to line 50             |

---

# 4. Editing

| Command | Action                  |
| ------- | ----------------------- |
| `x`     | Delete character        |
| `dd`    | Delete line             |
| `D`     | Delete to end of line   |
| `dw`    | Delete word             |
| `diw`   | Delete current word     |
| `cw`    | Change word             |
| `cc`    | Change entire line      |
| `C`     | Change till end of line |
| `r`     | Replace one character   |
| `R`     | Replace mode            |

---

# 5. Copy & Paste

| Command | Action       |
| ------- | ------------ |
| `yy`    | Copy line    |
| `yw`    | Copy word    |
| `p`     | Paste after  |
| `P`     | Paste before |

---

# 6. Undo & Redo

| Command  | Action |
| -------- | ------ |
| `u`      | Undo   |
| `Ctrl+r` | Redo   |

---

# 7. Search

| Command | Action                   |
| ------- | ------------------------ |
| `/word` | Search                   |
| `n`     | Next result              |
| `N`     | Previous result          |
| `*`     | Search word under cursor |

---

# 8. Visual Mode

| Command  | Action              |
| -------- | ------------------- |
| `v`      | Character selection |
| `V`      | Line selection      |
| `Ctrl+v` | Block selection     |
| `y`      | Copy selection      |
| `d`      | Delete selection    |

---

# 9. Window Management

| Command    | Action           |
| ---------- | ---------------- |
| `:split`   | Horizontal split |
| `:vsplit`  | Vertical split   |
| `Ctrl+w h` | Left window      |
| `Ctrl+w l` | Right window     |
| `Ctrl+w k` | Upper window     |
| `Ctrl+w j` | Lower window     |
| `Ctrl+w q` | Close window     |

---

# 10. Buffers

| Command | Action          |
| ------- | --------------- |
| `:bn`   | Next buffer     |
| `:bp`   | Previous buffer |
| `:bd`   | Close buffer    |

---

# 11. Tabs

| Command     | Action       |
| ----------- | ------------ |
| `:tabnew`   | New tab      |
| `gt`        | Next tab     |
| `gT`        | Previous tab |
| `:tabclose` | Close tab    |

---

# 12. Find & Replace

Replace all:

```vim
:%s/old/new/g
```

Replace in current line:

```vim
:s/old/new/g
```

---

# 13. File Explorer (LazyVim)

| Shortcut   | Action               |
| ---------- | -------------------- |
| `<Space>e` | Toggle file explorer |

---

# 14. Telescope (Best Feature)

| Shortcut    | Action                 |
| ----------- | ---------------------- |
| `<Space>ff` | Find files             |
| `<Space>fg` | Search text in project |
| `<Space>fr` | Recent files           |
| `<Space>fb` | Buffers                |
| `<Space>fh` | Help                   |

---

# 15. Git

| Shortcut    | Action              |
| ----------- | ------------------- |
| `<Space>gg` | LazyGit             |
| `]h`        | Next Git change     |
| `[h`        | Previous Git change |

---

# 16. LSP (Autocomplete)

| Shortcut    | Action               |
| ----------- | -------------------- |
| `gd`        | Go to definition     |
| `gr`        | References           |
| `K`         | Documentation        |
| `gi`        | Go to implementation |
| `<Space>ca` | Code action          |
| `<Space>cr` | Rename symbol        |
| `]d`        | Next error           |
| `[d`        | Previous error       |

---

# 17. Formatting

| Shortcut    | Action      |
| ----------- | ----------- |
| `<Space>cf` | Format file |

---

# 18. Comment

Most languages:

```text
gcc
```

Toggle current line.

Multiple lines:

```text
gc
```

in Visual Mode.

---

# 19. Terminal

| Shortcut        | Action                 |
| --------------- | ---------------------- |
| `<Space>ft`     | Floating terminal      |
| `exit`          | Close terminal session |
| `Ctrl+\ Ctrl+n` | Leave terminal mode    |

---

# 20. Useful Commands

| Command        | Action         |
| -------------- | -------------- |
| `:Lazy`        | Plugin manager |
| `:Mason`       | Install LSPs   |
| `:checkhealth` | Diagnose setup |
| `:help`        | Help           |
| `:q`           | Quit           |
| `:w`           | Save           |

---

# ⭐ Daily Workflow (for C++ & LeetCode)

```text
nvim 198.house-robber.cpp

Space e      -> File explorer
Space ff     -> Find files
i            -> Start typing
Esc          -> Stop typing
yy           -> Copy line
dd           -> Delete line
p            -> Paste
u            -> Undo
Ctrl+r       -> Redo
gd           -> Go to definition
K            -> Documentation
Space cf     -> Format code
:w           -> Save
:q           -> Quit
```

---

## 🎯 Learn these first (80/20 rule)

If you memorize just these, you'll be productive very quickly:

* `i`, `a`, `o`, `Esc`
* `:w`, `:q`, `:wq`
* `h`, `j`, `k`, `l`
* `w`, `b`, `0`, `$`, `gg`, `G`
* `dd`, `yy`, `p`, `u`, `Ctrl+r`
* `v`, `V`
* `/`, `n`
* `<Space>e`
* `<Space>ff`
* `gd`, `K`
* `<Space>cf`

These cover the vast majority of everyday editing tasks in LazyVim. Once you're comfortable with them, you can gradually add more advanced motions and text objects like `ciw`, `di(`, `ci"`, and macros.


- using - space+e we can open file explore
- using - using shift+ l/h we can move to next or prev tab



1. i saw your project and i wanted to aske some follow ups question regarding some of the concepts
2. section-aware chunking instead of fixed-size chunking -> in my project i use fixed size chunking -> i would like to aske question about section aware chunking is that what is one section is very small and other section is contain large text ,means how we can design this variable chunking

3. and during storing the embeddings in the vector db , you also store the metadata of that chunk, that information does it contains.

4. how router works and i know it will improve the accuracy of the system, as it will provide the exact information from which model it should call, is it makes call to db for checking for query validation 
5. and is it realy neccessary to use sparse retrival even if we are using dense retrival.
6. and if we are using these both retival techniques then what is both the tetrival techniques find diff chucks when what we do .
7. and we are manually uploading the sec filling ducuments or we make some api 
8. Can you explain the document ingestion process? Is it manual, or does the system automatically fetch new SEC filings, generate embeddings, and update the vector database?"