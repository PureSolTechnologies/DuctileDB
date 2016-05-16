import {Component} from 'angular2/core';

import {HighlighterComponent} from './commons/highlighter.component';
import {DocumentationSelectorComponent} from './documentation-selector.component';

@Component({
	selector: 'documentation-compile',
	directives: [
	  HighlighterComponent,
	  DocumentationSelectorComponent,
	],
	templateUrl: '../html/documentation-maven.html'
})
export class DocumentationMavenComponent {
}

