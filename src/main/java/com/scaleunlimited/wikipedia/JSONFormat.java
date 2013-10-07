package com.scaleunlimited.wikipedia;

public enum JSONFormat {
    BASIC_OUTPUT(),
    RABBIT_FISH(),
    BADGER_FISH();
    
    /** The encoding format */
    private String encoding;

    /** New line separator */
    private String lineSeparator = "\n";
    
    /** Should XML namespaces be included? */
    private boolean useNamespaces = false;

    private JSONFormat() {
      this("UTF-8");
    }
    
    private JSONFormat(String encoding) {
      this.encoding = encoding;
    }

    public String getEncoding() {
      return this.encoding;
    }

    public String getLineSeparator() {
      return this.lineSeparator;
    }

    public boolean getUseNamespaces() {
      return this.useNamespaces;
    }
    
    public String getName() {
      if (this.equals(BASIC_OUTPUT)) {
        return "Basic Output";
      } else if (this.equals(RABBIT_FISH)) {
        return "RabbitFish";
      } else if (this.equals(BADGER_FISH)) {
        return "BadgerFish";
      } else {
        throw new RuntimeException("Unknown JSONFormat");
      }
    }

  }
