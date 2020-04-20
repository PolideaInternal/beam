<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Contribution Guide

This guide consists of:

- [Project structure](#project-structure)
- [How to add a new blogpost](#how-to-add-a-new-blogpost)
- [How to add a new page](#how-to-add-a-new-page)
- [How to write in Hugo way](#how-to-write-in-hugo-way)
  - [Define TableOfContents](#define-tableofcontents)
  - [Language switching](#language-switching)
  - [Code highlighting](#code-highlighting)
  - [Adding class to markdown text](#paragraph)
  - [Table](#table)
  - [Github sample](#github-sample)
  - [Others](#others)

## Project structure

```
www/
├── dist                                  # bundle files
├── site
│   ├── archetypes                        # frontmatter template
│   ├── assets
│   │   └── scss                          # styles
│   ├── content                           # pages
│   │   └── en
│   │       ├── blog
│   │       ├── community
│   │       ├── contribute
│   │       ├── documentation
│   │       ├── get-started
│   │       ├── privacy_policy
│   │       ├── roadmap
│   │       └── security
│   │       └── _index.md
│   ├── data
│   ├── layouts                           # content template
│   ├── static
│   │   ├── downloads                     # downloaded files
│   │   └── fonts
│   │   └── images
│   │   └── js
│   └── themes
│       └── docsy
├── check-links.sh                        # links checker
└── package.json
```

## How to add a new blogpost

To add a new blogpost with pre-filled frontmatter, in `/www/site` run:

```
$ hugo new blog/my-new-blogpost.md
```

That will create a markdown file `/www/site/content/<LANGUAGE_VERSION>/blog/my-new-blogpost.md` with following content:

```
---
title: "My New Blogpost"
date: "2020-04-20T14:02:57+02:00"
categories: 
  - blog
authors: 
  - "Your Name"
---
```

Below frontmatter, put your blogpost content. The filename will also serve as URL for your blogpost as `/blog/{filename}`.

## How to add a new page

For example, you would like to add a new `About` page.

First, you need to create a markdown file in `/www/site/content/<LANGUAGE_VERSION>/about/_index.md` with following content:

```
---
title: "Your page title"
---
```

Below frontmatter, put your page content. The filename will also serve as URL for your page as `/about`.

Second, define your page layout in the `layout` section with the same structure `/www/site/layout/about/{your_template}`. Hugo will help you to pick up the template behind the scene. Please refer to [Hugo documentation](https://gohugo.io/templates/) for the usage of templates.

## How to write in Hugo

This section will guide you how to use Hugo shortcodes in Apache Beam website. Please refer to [Hugo documentation](https://gohugo.io/content-management/shortcodes/) for more details of usage.

### Define TableOfContents

To automatically generate table of contents in a markdown file. Simply use:

```
{{< toc >}}
```

### Language switching

To have a programming language tab switcher, for instance of java, python and go. Use:

```
{{< language-switchers java py go >}}
```

### Code highlighting

To be consistent, please prefer to use `{{< highlight >}}` syntax instead of ` ``` `, for code-blocks or syntax-highlighters.

1. To apply code highlighting to java, python or go. Use:

```
{{< highlight java >}}
// This is java
{{< /highlight >}}
```

2. To apply code highlighting to a wrapper class. Use:

```
{{< highlight class="runner-direct" >}}
// This is java
{{< /highlight >}}
```

The purpose of adding classes or programming languages (java, py or go) in code highlighting is to activate the language switching feature.

### Adding class to markdown text

1. To add a class to an inline text. Use:

```
{{< paragraph class="java-language">}}
This is an inline markdown text.
{{< /paragraph >}}
```

2. To add a class to a block text. Use:

```
{{< paragraph class="java-language" wrap="span">}}
- This is the first text.
- This is the second text.
- This is the third text.
{{< /paragraph >}}
```

The purpose of adding classes in markdown text is to activate the language switching feature.

### Table

If you would like to use the table markdown syntax but also have bootstrap table styles. Wrap your table markdown inside:

```
{{< table >}}
A table markdown here.
{{< /table >}}
```

### Github sample

To retrieve a piece of code in github.

Usage:

```
{{< github_sample /path/to/file selected_tag >}}
```

Example:

```
{{< github_sample "/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/user_score.py" extract_and_sum_score >}}
```

### Others

To get released latest version in markdown:

```
{{< param release_latest >}}
```

To get branch of the repository in markdown:

```
{{< param branch_repo >}}
```

<!-- 
  TODO: Add a repository link of an example when be merged to master
-->
To render capability matrix, please take a look at [this example]().
