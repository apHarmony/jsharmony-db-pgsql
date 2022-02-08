module.exports = {
    "env": {
        "browser": true,
    },
    "plugins": [
        "es5"
    ],
    "extends": [
        "eslint:recommended",
        "plugin:es5/no-es2015"
    ],
    "globals": {
      "it": "readonly",
      "describe": "readonly",
      "after": "readonly",
      "before": "readonly",
    },
    "rules": {
        "no-trailing-spaces":[
          "error",
          { "skipBlankLines": true }
        ],
        "indent": [
            "error",
            2,
            { "SwitchCase":1 }
        ],
        "linebreak-style": [
            "error",
            "windows"
        ],
        "quotes": "off",
        "semi": [
            "error",
            "always"
        ],
        "no-console": "off",
        "no-unused-vars": [
          "error",
          {
            "args": "none"
          }
        ],
        "no-cond-assign": [
          "error",
          "except-parens"
        ]
    }
};