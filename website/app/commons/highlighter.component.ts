import {Component, Input} from 'angular2/core';

declare var $: any;
declare var hljs: any;

@Component({
  selector: 'highlighter',
  template: `
    <div style="border:1pt solid gray;padding:5pt;">
      <pre><code class="{{lang}} language-{{lang}}" data-lang="{{lang}}"><ng-content></ng-content></code></pre>
      <p>{{title}}</p>
    </div>
`
})
export class HighlighterComponent {

  @Input() lang: String;
  @Input() title: String;

  ngAfterContentInit() {
    $('pre code').each(function(i, block) {
      hljs.highlightBlock(block);
    });
  }
}
