import {Component} from 'angular2/core';

@Component({
	selector: 'documentation',
	templateUrl: '../html/documentation.html'
})
export class DocumentationComponent {
  ngAfterContentInit() {
    $('pre code').each(function(i, block) {
      hljs.highlightBlock(block);
    });
  //hljs.initHighlighting();
  }
}
